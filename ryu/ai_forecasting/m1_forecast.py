import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import time
import sys
import gdown
import os

# --- Impor Library Baru untuk TFT (VERSI 2.0) ---
import torch
import lightning.pytorch as pl  # DIUBAH: dari pytorch_lightning
from lightning.pytorch.callbacks import EarlyStopping  # DIUBAH: dari pytorch_lightning.callbacks
from pytorch_forecasting import (
    TimeSeriesDataSet,
    TemporalFusionTransformer,
    QuantileLoss,
    GroupNormalizer
)
# ------------------------------------

# --- 3. Fungsi Utama (Main Execution) ---
def main():
    
    # --- 1. KONFIGURASI DAN DOWNLOAD DATA ---
    file_id = "1RcSKE6g62QXKCWztCf4W28VcZvq-vqDN"
    url = f"https://drive.google.com/uc?id={file_id}"
    FILE_NAME = "data_center_traffic_v1.csv"
    
    if not os.path.exists(FILE_NAME):
        print(f"File '{FILE_NAME}' tidak ditemukan. Mendownload dari Google Drive...")
        gdown.download(url, FILE_NAME, quiet=False)
    else:
        print(f"File '{FILE_NAME}' sudah ada, proses download dilewati.")
    
    CATEGORY_TO_FORECAST = 'web_app' # Ganti ini ke 'web' atau kategori lain
    TARGET = 'bytes_tx'
    # --------------------

    # --- 2. Memuat dan Memproses Data ---
    try:
        print(f"Memuat data dari {FILE_NAME}...")
        df = pd.read_csv(FILE_NAME) 
    except Exception as e:
        print(f"Error saat memuat CSV: {e}")
        sys.exit(1)

    # Cek kolom-kolom penting
    if 'timestamp' not in df.columns or TARGET not in df.columns or 'category' not in df.columns:
         print("ERROR: Kolom 'timestamp', 'category', atau 'bytes_tx' tidak ditemukan.")
         sys.exit(1)

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    if CATEGORY_TO_FORECAST not in df['category'].unique():
        print(f"ERROR: Kategori '{CATEGORY_TO_FORECAST}' tidak ditemukan.")
        sys.exit(1)

    print(f"Data mentah dimuat: {len(df)} baris.")
    
    # --- 3. ISOLASI DAN RESAMPLING PER KATEGORI ---
    print(f"Mengisolasi dan memproses data untuk kategori: '{CATEGORY_TO_FORECAST}'")
    
    df_cat = df[df['category'] == CATEGORY_TO_FORECAST].copy()
    
    if df_cat.empty:
        print(f"Tidak ada data ditemukan untuk kategori '{CATEGORY_TO_FORECAST}'.")
        sys.exit(1)

    df_cat = df_cat.set_index('timestamp')
    print("Melakukan resampling data ke interval 5 detik...")
    
    # PERBAIKAN Peringatan: Ubah '5S' menjadi '5s'
    df_resampled = df_cat.resample('5s')[TARGET].sum().to_frame()
    
    df_resampled[TARGET] = df_resampled[TARGET].fillna(0)
    
    # PERBAIKAN ERROR torch.finfo
    df_resampled[TARGET] = df_resampled[TARGET].astype(np.float32)
    
    print(f"Data setelah di-resample: {len(df_resampled)} baris.")
    
    # --- 4. Feature Engineering untuk TFT ---
    print("Mempersiapkan data untuk Temporal Fusion Transformer...")
    df_proc = df_resampled.reset_index()
    
    # Tentukan parameter utama
    FORECAST_STEPS = (60 * 60) // 5 # 1 jam = 720 langkah
    LOOKBACK_STEPS = FORECAST_STEPS * 3 # Gunakan 3 jam histori
    
    # 1. Buat 'time_idx' (indeks integer yang terus bertambah)
    min_ts = df_proc['timestamp'].min()
    df_proc['time_idx'] = ((df_proc['timestamp'] - min_ts).dt.total_seconds() / 5).astype(int)

    # 2. Buat fitur 'waktu' yang diketahui di masa depan
    df_proc['hour'] = df_proc.timestamp.dt.hour
    df_proc['minute'] = df_proc.timestamp.dt.minute
    df_proc['dayofweek'] = df_proc.timestamp.dt.dayofweek
    
    # 3. Buat 'group_id' (TFT membutuhkannya, walau kita hanya punya 1)
    df_proc['category'] = CATEGORY_TO_FORECAST

    # 4. Buat TimeSeriesDataSet
    # Kita akan latih model di semua data, lalu gunakan data terakhir untuk prediksi
    dataset = TimeSeriesDataSet(
        df_proc,
        time_idx="time_idx",
        target=TARGET,
        group_ids=["category"],
        max_encoder_length=LOOKBACK_STEPS,
        max_prediction_length=FORECAST_STEPS,
        # Fitur statis (tidak berubah seiring waktu)
        static_categoricals=["category"],
        # Fitur yang berubah, tapi kita tahu di masa depan (jam, hari, dll)
        time_varying_known_reals=["hour", "minute", "dayofweek"],
        # Fitur yang berubah, dan kita TIDAK tahu di masa depan (target itu sendiri)
        time_varying_unknown_reals=[TARGET],
        # Normalisasi target (PENTING untuk deep learning)
        target_normalizer=GroupNormalizer(groups=["category"], transformation="softplus"),
        add_relative_time_idx=True,
    )
    
    # Buat DataLoader
    train_loader = dataset.to_dataloader(train=True, batch_size=32, num_workers=0)

    # --- 5. Pelatihan Model TFT ---
    print("\nMelatih model Temporal Fusion Transformer...")
    print("PERINGATAN: Ini bisa memakan waktu beberapa menit.")
    start_train_time = time.time()
    
    # Gunakan Pytorch Lightning Trainer
    trainer = pl.Trainer(
        max_epochs=5,
        # --- PERBAIKAN 1 ---
        # accelerator="auto", # Jangan gunakan auto, paksa CPU
        accelerator="cpu",  # Paksa CPU untuk PELATIHAN
        # -------------------
        gradient_clip_val=0.1,
        limit_train_batches=50, 
        callbacks=[EarlyStopping(monitor="train_loss", min_delta=1e-4, patience=3, verbose=False, mode="min")],
    )
    
    # Inisialisasi model TFT
    tft = TemporalFusionTransformer.from_dataset(
        dataset,
        learning_rate=0.01,
        hidden_size=8,
        attention_head_size=1,
        dropout=0.1,
        hidden_continuous_size=4,
        loss=QuantileLoss(),
    )
    
    # Latih model
    trainer.fit(tft, train_dataloaders=train_loader)
    
    end_train_time = time.time()
    print(f"Pelatihan model selesai dalam {end_train_time - start_train_time:.2f} detik.")

    # --- 6. Proses Peramalan (Forecasting) 1 Jam ke Depan ---
    print(f"\nMemulai peramalan 1 jam ke depan untuk '{CATEGORY_TO_FORECAST}'...")
    start_forecast_time = time.time()
    
    # --- PERBAIKAN 2 (Paling Penting) ---
    # Beritahu tft.predict() untuk HANYA menggunakan CPU saat meramal
    raw_predictions = tft.predict(
        df_proc, 
        trainer_kwargs={"accelerator": "cpu"} # Paksa CPU untuk PERAMALAN
    )
    # --- AKHIR PERBAIKAN ---
    
    # Hasilnya adalah 1 prediksi untuk 1 grup, ambil [0]
    future_predictions = raw_predictions[0].cpu().numpy()
    
    end_forecast_time = time.time()
    print(f"Peramalan 720 langkah selesai dalam {end_forecast_time - start_forecast_time:.2f} detik.")

    # --- 7. Membuat Grafik ---
    print("Menampilkan grafik hasil peramalan...")
    
    # Buat index waktu untuk peramalan
    last_timestamp = df_proc['timestamp'].iloc[-1]
    forecast_index = pd.date_range(
        start=last_timestamp + pd.Timedelta(seconds=5),
        periods=FORECAST_STEPS,
        freq='5s' # Sudah diubah dari 5S
    )
    
    forecast_series = pd.Series(future_predictions, index=forecast_index)
    
    # Set data historis untuk di-plot (sama seperti di skrip XGBoost)
    y = df_proc.set_index('timestamp')[TARGET]
    history_to_plot = y.iloc[-FORECAST_STEPS * 3:] # Tampilkan 3 jam terakhir
    
    plt.figure(figsize=(15, 8))
    
    plt.plot(history_to_plot.index, history_to_plot, label=f'Data Historis ({CATEGORY_TO_FORECAST})')
    plt.plot(forecast_series.index, forecast_series, label=f'Peramalan 1 Jam (TFT)', color='red', linestyle='--')
    
    plt.title(f'Peramalan {TARGET} (TFT) untuk Kategori: {CATEGORY_TO_FORECAST}')
    plt.xlabel('Timestamp')
    plt.ylabel(TARGET)
    
    # --- Modifikasi Skala Waktu (Sumbu-X) ---
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    # --- Akhir Modifikasi ---
    
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    
    plot_filename = f"forecast_TFT_{CATEGORY_TO_FORECAST}.png"
    plt.savefig(plot_filename, dpi=300)
    print(f"Grafik disimpan sebagai: {plot_filename} (Resolusi Tinggi)")
    
    plt.show()

if __name__ == "__main__":
    pl.seed_everything(42) # Set seed untuk reproduktivitas
    main()