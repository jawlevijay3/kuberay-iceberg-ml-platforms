#!/usr/bin/env python3
"""
Local Pipeline Execution & Demonstration Script.
Demonstrates the complete workflow locally:
1. Ingestion of SupplyChain (1,200 records) and Financial (1,200 records) datasets
2. Entity Resolution via EntityResolver
3. Machine Learning model training for revenue prediction
4. Apache Iceberg table transactional upsert commit

# JM Comment: Local simulation runner for interviewing demonstrations and dry-run testing.
"""

import os
import sys
import time

# Add root directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.insert(0, root_dir)

from ray_pipeline import (
    EntityResolver,
    train_revenue_model,
    write_to_iceberg,
    logger
)
import csv

def run_local_demonstration():
    print("==========================================================================")
    print("  DISTRIBUTED AI FOUNDATION: PIPELINE DEMONSTRATION & LOCAL DRY-RUN       ")
    print("==========================================================================")
    
    s1_path = os.path.join(root_dir, "data", "SupplyChain_Data_S1.csv")
    s2_path = os.path.join(root_dir, "data", "Financial_Data_S2.csv")

    # Step 1: Ingestion
    print("\n[Step 1/4] Ingesting CSV datasets...")
    records = []
    for path, source_name in [(s1_path, "SupplyChain_Data_S1"), (s2_path, "Financial_Data_S2")]:
        with open(path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch = list(reader)
            for r in batch:
                r["source_feed"] = source_name
            records.extend(batch)
            print(f" -> Ingested {len(batch)} records from {source_name}")

    print(f" -> Total harmonized input records: {len(records)}")

    # Step 2: Entity Resolution
    print("\n[Step 2/4] Executing Entity Resolution via Simulated LLM...")
    resolver_cls = EntityResolver.__ray_metadata__.modified_class if hasattr(EntityResolver, "__ray_metadata__") else EntityResolver
    resolver = resolver_cls()
    
    start_time = time.time()
    resolved_records = resolver.resolve_batch(records)
    elapsed = time.time() - start_time
    print(f" -> Entity Resolution completed in {elapsed:.2f}s for {len(resolved_records)} records.")
    print(f" -> Sample resolved records:")
    for r in resolved_records[:4]:
        print(f"    * ID: {r['canonical_id']:<15} | Raw: {r['corporate_name'][:28]:<30} | Canon: {r['canonical_name']}")

    # Step 3: Model Training
    print("\n[Step 3/4] Training Machine Learning Revenue Prediction Model...")
    metrics = train_revenue_model(resolved_records)
    print(f" -> Model Trained Successfully!")
    print(f"    * Model Type: {metrics.get('model_type')}")
    print(f"    * R² Score:   {metrics.get('test_r2_score')}")
    print(f"    * Test RMSE:  ${metrics.get('test_rmse'):,.2f}")

    # Step 4: Iceberg Sink
    print("\n[Step 4/4] Committing Transaction to Apache Iceberg Table...")
    iceberg_res = write_to_iceberg(resolved_records, table_name="corporate_registry")
    if iceberg_res:
        print(" -> ACID Transaction committed successfully with snapshot isolation!")

    print("\n==========================================================================")
    print("  DEMONSTRATION COMPLETED SUCCESSFULLY (100% OPERATIONAL)                 ")
    print("==========================================================================")

if __name__ == "__main__":
    run_local_demonstration()
