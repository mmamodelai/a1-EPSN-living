#!/usr/bin/env python3
"""
Universal Dossier Mapping System Demo
=====================================

This script demonstrates how to map all ESPN data to create professional
fighter dossiers for ANY fighter in the system.

Usage:
    python demo_universal_mapping.py [fighter_name]
"""

import sys
from create_universal_dossier_system import UniversalDossierSystem

def demo_single_fighter(fighter_name):
    """Demonstrate creating a dossier for a single fighter."""
    
    print(f"🎯 UNIVERSAL DOSSIER MAPPING DEMO")
    print(f"=================================")
    print(f"Fighter: {fighter_name}")
    print(f"")
    
    # Initialize the system
    dossier_system = UniversalDossierSystem()
    
    # Create the dossier
    result = dossier_system.create_fighter_dossier(fighter_name)
    
    if result:
        print(f"\n✅ SUCCESS: Dossier created for {fighter_name}")
        print(f"📁 File: {result['filename']}")
        
        # Show data mapping summary
        profile = result['fighter_profile'].iloc[0]
        print(f"\n📊 DATA MAPPING SUMMARY:")
        print(f"  Career Strikes: {profile['Sig. Strikes Landed']} landed, {profile['Striking accuracy']}% accuracy")
        print(f"  Career Takedowns: {profile['Takedowns Landed']} landed, {profile['Takedown Accuracy']}% accuracy")
        print(f"  Win Methods: {profile['Win by Method - KO/TKO']} KO/TKO, {profile['Win by Method - SUB']} SUB, {profile['Win by Method - DEC']} DEC")
        print(f"  Target Breakdown: {profile['Sig. Str. by target - Head Strike Percentage']}% head, {profile['Sig. Str. by target - Body Strike Percentage']}% body, {profile['Sig. Str. by target - Leg Strike Percentage']}% legs")
        print(f"  Total Fights: {len(result['fight_results'])}")
        print(f"  Latest Fight: {result['fight_results']['Date'].iloc[0] if len(result['fight_results']) > 0 else 'N/A'}")
        
    else:
        print(f"❌ FAILED: Could not create dossier for {fighter_name}")

def demo_multiple_fighters(fighter_names):
    """Demonstrate creating dossiers for multiple fighters."""
    
    print(f"🎯 BATCH DOSSIER CREATION DEMO")
    print(f"==============================")
    print(f"Fighters: {', '.join(fighter_names)}")
    print(f"")
    
    # Initialize the system
    dossier_system = UniversalDossierSystem()
    
    # Create dossiers for all fighters
    results = dossier_system.create_multiple_dossiers(fighter_names)
    
    print(f"\n📋 BATCH RESULTS:")
    for result in results:
        if result:
            profile = result['fighter_profile'].iloc[0]
            print(f"  ✅ {profile['Name']}: {profile['Sig. Strikes Landed']} strikes, {len(result['fight_results'])} fights")

def show_available_fighters():
    """Show a sample of available fighters."""
    
    print(f"📋 AVAILABLE FIGHTERS SAMPLE")
    print(f"============================")
    
    dossier_system = UniversalDossierSystem()
    fighters = dossier_system.get_available_fighters()
    
    print(f"Total fighters available: {len(fighters)}")
    print(f"")
    print(f"Sample fighters:")
    for i, fighter in enumerate(fighters[:20]):  # Show first 20
        print(f"  {i+1:2d}. {fighter}")
    
    if len(fighters) > 20:
        print(f"  ... and {len(fighters) - 20} more")

def main():
    """Main function."""
    
    if len(sys.argv) > 1:
        # Single fighter mode
        fighter_name = sys.argv[1]
        demo_single_fighter(fighter_name)
    else:
        # Demo mode - show available fighters and create sample dossiers
        show_available_fighters()
        
        print(f"\n" + "="*50)
        print(f"DEMO: Creating sample dossiers...")
        print(f"="*50)
        
        # Create dossiers for some well-known fighters
        demo_fighters = [
            "Robert Whittaker",
            "Israel Adesanya", 
            "Paulo Costa",
            "Dricus Du Plessis",
            "Khamzat Chimaev"
        ]
        
        demo_multiple_fighters(demo_fighters)
        
        print(f"\n" + "="*50)
        print(f"🎯 UNIVERSAL MAPPING SYSTEM READY!")
        print(f"="*50)
        print(f"")
        print(f"To create a dossier for any fighter:")
        print(f"  python demo_universal_mapping.py \"Fighter Name\"")
        print(f"")
        print(f"Example:")
        print(f"  python demo_universal_mapping.py \"Robert Whittaker\"")
        print(f"  python demo_universal_mapping.py \"Israel Adesanya\"")
        print(f"  python demo_universal_mapping.py \"Paulo Costa\"")

if __name__ == "__main__":
    main() 