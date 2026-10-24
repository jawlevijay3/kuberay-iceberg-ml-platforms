#!/usr/bin/env python3
"""
Synthetic Corporate Dataset Generator for Ray Distributed Pipeline.
Generates two distinct datasets with >= 1,200 records each using pure Python standard library:
1. SupplyChain_Data_S1.csv (Supply chain, logistics, vendor spend, risk score)
2. Financial_Data_S2.csv (Corporate financials, balance sheet, EBITDA, legal entity)

# JM Comment: Enterprise data generator ensuring realistic entity overlap for Entity Resolution evaluation.
"""

import os
import csv
import math
import random

# JM Comment: Deterministic random seed for reproducible benchmark datasets
random.seed(42)

BASE_CORPORATE_NAMES = [
    ("Acme Corp", "Acme Corporation Inc", "ACME Global Logistics"),
    ("Vertex Technologies", "Vertex Tech Solutions", "Vertex Dynamics Corp"),
    ("Apex Global Logistics", "Apex Worldwide Logistics Ltd", "Apex Cargo Systems"),
    ("Nexus Healthcare", "Nexus Health & BioSciences", "Nexus Medical Devices Inc"),
    ("Horizon Renewable Power", "Horizon Green Energy LLC", "Horizon Clean Power Systems"),
    ("Pinnacle Financial Group", "Pinnacle Capital Partners", "Pinnacle Wealth Management"),
    ("Titan Heavy Industries", "Titan Industrial Manufacturing", "Titan Machinery & Robotics"),
    ("Aura Digital Media", "Aura Communications Group", "Aura Interactive Systems"),
    ("Sovereign Mining Corp", "Sovereign Mineral Resources", "Sovereign Metals Ltd"),
    ("Omega Defense Dynamics", "Omega Aerospace & Defense", "Omega Tactical Systems"),
    ("Sterling Retail Holdings", "Sterling Consumer Products", "Sterling Supermarkets Ltd"),
    ("Quantum Cloud Systems", "Quantum Data Infrastructure", "Quantum AI Compute Corp"),
    ("Vanguard Agricultural Tech", "Vanguard BioAgri Solutions", "Vanguard Precision Farming"),
    ("Crestview Real Estate", "Crestview Commercial Properties", "Crestview REIT Management"),
    ("Solstice Maritime Logistics", "Solstice Shipping Line", "Solstice Ocean Freight"),
    ("Hyperion BioPharm", "Hyperion Life Sciences", "Hyperion Therapeutics Inc"),
    ("Strata Semiconductor", "Strata Microelectronics Corp", "Strata Silicon Foundries"),
    ("Elysium Automotive", "Elysium Mobility & EV Group", "Elysium Electric Motors"),
    ("Beacon Cybersecurity", "Beacon Threat Intelligence", "Beacon ZeroTrust Networks"),
    ("Cascade Water & Utilities", "Cascade Environmental Services", "Cascade Infrastructure Corp")
]

SECTORS = [
    "Technology", "Industrial", "Logistics", "Healthcare", "Energy",
    "Financial Services", "Retail", "Semiconductor", "Automotive", "Defense"
]

COUNTRIES = ["USA", "GBR", "DEU", "JPN", "SGP", "CAN", "AUS", "FRA", "NLD", "CHE"]

def sample_lognormal(mean=7.5, sigma=1.2):
    # Box-Muller transform for standard normal
    u1 = max(1e-10, random.random())
    u2 = random.random()
    z0 = math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)
    return math.exp(mean + sigma * z0)

def generate_datasets(output_dir: str, num_records: int = 1200):
    os.makedirs(output_dir, exist_ok=True)
    
    # JM Comment: Generate 1,200 Supply Chain records
    s1_path = os.path.join(output_dir, "SupplyChain_Data_S1.csv")
    s1_headers = [
        "record_id", "corporate_name", "vendor_tier", "headcount",
        "procurement_spend_usd", "supply_chain_risk_score", "country",
        "year_founded", "reported_revenue_usd"
    ]
    
    with open(s1_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=s1_headers)
        writer.writeheader()
        for i in range(num_records):
            cluster_idx = i % len(BASE_CORPORATE_NAMES)
            variant_idx = random.choice([0, 1, 2])
            raw_name = BASE_CORPORATE_NAMES[cluster_idx][variant_idx]
            
            if random.random() < 0.15:
                raw_name += f" - Division {random.randint(1, 9)}"
                
            headcount = int(sample_lognormal(7.5, 1.2)) + 50
            spend_usd = float(headcount * random.uniform(15000, 45000) + random.uniform(400000, 600000))
            supply_chain_risk = round(random.betavariate(2, 5), 3)
            year_founded = random.randint(1965, 2021)
            tier = random.choice(["Tier 1", "Tier 2", "Strategic Vendor", "Critical Partner"])
            
            latent_rev = spend_usd * random.uniform(2.8, 4.2) + headcount * 25000
            
            writer.writerow({
                "record_id": f"SC-{10000 + i}",
                "corporate_name": raw_name,
                "vendor_tier": tier,
                "headcount": headcount,
                "procurement_spend_usd": round(spend_usd, 2),
                "supply_chain_risk_score": supply_chain_risk,
                "country": random.choice(COUNTRIES),
                "year_founded": year_founded,
                "reported_revenue_usd": round(latent_rev, 2)
            })
            
    print(f"Successfully generated {num_records} records in {s1_path}")

    # JM Comment: Generate 1,200 Financial Records
    s2_path = os.path.join(output_dir, "Financial_Data_S2.csv")
    s2_headers = [
        "filing_id", "corporate_name", "sector", "operating_cost_usd",
        "ebitda_usd", "debt_to_equity_ratio", "audit_compliance_score",
        "country", "reported_revenue_usd"
    ]
    
    with open(s2_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=s2_headers)
        writer.writeheader()
        for i in range(num_records):
            cluster_idx = i % len(BASE_CORPORATE_NAMES)
            variant_idx = random.choice([0, 1, 2])
            raw_name = BASE_CORPORATE_NAMES[cluster_idx][variant_idx]
            
            if random.random() < 0.10:
                raw_name = raw_name.upper()
                
            sector = SECTORS[cluster_idx % len(SECTORS)]
            headcount_proxy = int(sample_lognormal(7.5, 1.2)) + 50
            ebitda_margin = random.uniform(0.12, 0.35)
            debt_to_equity = round(random.expovariate(1.0 / 1.1), 2)
            operating_cost = float(headcount_proxy * random.uniform(20000, 50000))
            
            latent_rev = (operating_cost / (1.0 - ebitda_margin)) * random.uniform(0.9, 1.15)
            ebitda_usd = latent_rev * ebitda_margin
            
            writer.writerow({
                "filing_id": f"SEC-2024-{20000 + i}",
                "corporate_name": raw_name,
                "sector": sector,
                "operating_cost_usd": round(operating_cost, 2),
                "ebitda_usd": round(ebitda_usd, 2),
                "debt_to_equity_ratio": debt_to_equity,
                "audit_compliance_score": round(random.uniform(82.0, 99.8), 2),
                "country": random.choice(COUNTRIES),
                "reported_revenue_usd": round(latent_rev, 2)
            })

    print(f"Successfully generated {num_records} records in {s2_path}")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    out = os.path.join(os.path.dirname(current_dir), "data")
    generate_datasets(out, num_records=1200)
