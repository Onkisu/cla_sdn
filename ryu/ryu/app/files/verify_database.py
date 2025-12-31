#!/usr/bin/env python3
"""
Database Verification Script
Check database connection and data integrity
"""

import psycopg2
from datetime import datetime, timedelta

# Database configuration
DB_CONFIG = {
    'host': '103.181.142.165',
    'database': 'development',
    'user': 'dev_one',
    'password': 'hijack332.',
    'port': 5432
}

def test_connection():
    """Test database connection"""
    print("="*70)
    print("Testing Database Connection...")
    print("="*70)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ Connection successful!")
        print(f"📊 PostgreSQL version: {version[0]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def check_table_exists():
    """Check if table exists"""
    print("\n" + "="*70)
    print("Checking Table...")
    print("="*70)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'traffic' 
                AND table_name = 'flow_stats_'
            );
        """)
        
        exists = cursor.fetchone()[0]
        
        if exists:
            print("✅ Table 'traffic.flow_stats_' exists")
            
            # Get row count
            cursor.execute("SELECT COUNT(*) FROM traffic.flow_stats_;")
            count = cursor.fetchone()[0]
            print(f"📊 Total records: {count}")
            
        else:
            print("⚠️  Table 'traffic.flow_stats_' does not exist")
            print("   It will be created when simulation runs")
        
        cursor.close()
        conn.close()
        return exists
        
    except Exception as e:
        print(f"❌ Error checking table: {e}")
        return False

def verify_data_integrity():
    """Verify data integrity"""
    print("\n" + "="*70)
    print("Verifying Data Integrity...")
    print("="*70)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Check if table has data
        cursor.execute("SELECT COUNT(*) FROM traffic.flow_stats_;")
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("⚠️  No data in table yet")
            print("   Run the simulation to generate data")
            return
        
        # Check bytes_tx range
        cursor.execute("""
            SELECT 
                MIN(bytes_tx) as min_bytes,
                MAX(bytes_tx) as max_bytes,
                AVG(bytes_tx) as avg_bytes
            FROM traffic.flow_stats_;
        """)
        
        min_bytes, max_bytes, avg_bytes = cursor.fetchone()
        
        print(f"\n📊 bytes_tx Statistics:")
        print(f"   Min: {min_bytes} bytes")
        print(f"   Max: {max_bytes} bytes")
        print(f"   Avg: {avg_bytes:.2f} bytes")
        
        # Verify range
        if 13000 <= min_bytes and max_bytes <= 19800:
            print("   ✅ All bytes_tx values in valid range (13000-19800)")
        else:
            print("   ⚠️  Some bytes_tx values outside expected range")
        
        # Check for NULL values
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(timestamp) as has_timestamp,
                COUNT(dpid) as has_dpid,
                COUNT(src_ip) as has_src_ip,
                COUNT(dst_ip) as has_dst_ip,
                COUNT(bytes_tx) as has_bytes_tx,
                COUNT(traffic_label) as has_label
            FROM traffic.flow_stats_;
        """)
        
        stats = cursor.fetchone()
        total = stats[0]
        
        print(f"\n📊 Data Completeness:")
        all_complete = True
        for i, col in enumerate(['timestamp', 'dpid', 'src_ip', 'dst_ip', 'bytes_tx', 'traffic_label']):
            if stats[i+1] == total:
                print(f"   ✅ {col}: {stats[i+1]}/{total}")
            else:
                print(f"   ❌ {col}: {stats[i+1]}/{total} (missing {total - stats[i+1]})")
                all_complete = False
        
        if all_complete:
            print("\n   ✅ No NULL values found - data is complete!")
        
        # Check recent data
        cursor.execute("""
            SELECT timestamp, src_ip, dst_ip, bytes_tx, traffic_label
            FROM traffic.flow_stats_
            ORDER BY timestamp DESC
            LIMIT 5;
        """)
        
        print(f"\n📊 Latest 5 Records:")
        print(f"   {'Timestamp':<20} {'Src IP':<15} {'Dst IP':<15} {'Bytes TX':<10} {'Label':<10}")
        print(f"   {'-'*75}")
        
        for row in cursor.fetchall():
            ts, src, dst, bytes_tx, label = row
            print(f"   {str(ts):<20} {src:<15} {dst:<15} {bytes_tx:<10} {label:<10}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")

def show_traffic_summary():
    """Show traffic summary"""
    print("\n" + "="*70)
    print("Traffic Summary...")
    print("="*70)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Check if table has data
        cursor.execute("SELECT COUNT(*) FROM traffic.flow_stats_;")
        count = cursor.fetchone()[0]
        
        if count == 0:
            print("⚠️  No data available yet")
            return
        
        # Flows per IP pair
        cursor.execute("""
            SELECT 
                src_ip, 
                dst_ip, 
                COUNT(*) as flow_count,
                AVG(bytes_tx) as avg_bytes_tx,
                AVG(pkts_tx) as avg_pkts_tx
            FROM traffic.flow_stats_
            GROUP BY src_ip, dst_ip
            ORDER BY flow_count DESC
            LIMIT 10;
        """)
        
        print(f"\n📊 Top 10 Host Pairs by Flow Count:")
        print(f"   {'Src IP':<15} {'Dst IP':<15} {'Flows':<10} {'Avg Bytes':<12} {'Avg Pkts':<10}")
        print(f"   {'-'*70}")
        
        for row in cursor.fetchall():
            src, dst, count, avg_bytes, avg_pkts = row
            print(f"   {src:<15} {dst:<15} {count:<10} {avg_bytes:<12.0f} {avg_pkts:<10.0f}")
        
        # Time range
        cursor.execute("""
            SELECT 
                MIN(timestamp) as first_record,
                MAX(timestamp) as last_record
            FROM traffic.flow_stats_;
        """)
        
        first, last = cursor.fetchone()
        duration = (last - first).total_seconds()
        
        print(f"\n📊 Time Range:")
        print(f"   First record: {first}")
        print(f"   Last record:  {last}")
        print(f"   Duration:     {duration:.0f} seconds ({duration/60:.1f} minutes)")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error showing summary: {e}")

def main():
    """Main function"""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║" + " "*15 + "DATABASE VERIFICATION TOOL" + " "*27 + "║")
    print("╚" + "="*68 + "╝")
    
    # Test connection
    if not test_connection():
        print("\n❌ Cannot proceed without database connection")
        return
    
    # Check table
    table_exists = check_table_exists()
    
    # If table exists, verify data
    if table_exists:
        verify_data_integrity()
        show_traffic_summary()
    
    print("\n" + "="*70)
    print("✅ Verification complete!")
    print("="*70)
    print()

if __name__ == '__main__':
    main()
