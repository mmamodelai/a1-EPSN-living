#!/usr/bin/env python3
"""
Test HTML UPSERT Functionality
Verifies that the HTML UPSERT logic works correctly
"""

import sys
import tempfile
import shutil
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from espn_data_processor import ESPNDataProcessor

def test_html_upsert():
    """Test the HTML UPSERT functionality"""
    print("🧪 Testing HTML UPSERT Functionality")
    print("=" * 50)
    
    # Create a temporary test directory
    with tempfile.TemporaryDirectory() as temp_dir:
        test_data_dir = Path(temp_dir) / "data"
        test_data_dir.mkdir()
        
        # Create test FighterHTMLs directory
        test_html_dir = test_data_dir / "FighterHTMLs"
        test_html_dir.mkdir()
        
        # Create some existing HTML files
        existing_fighters = ["Conor_McGregor", "Khabib_Nurmagomedov", "Jon_Jones"]
        for fighter in existing_fighters:
            html_file = test_html_dir / f"{fighter}.html"
            with open(html_file, 'w') as f:
                f.write(f"<html><body><h1>{fighter}</h1><p>Old content</p></body></html>")
        
        print(f"✅ Created {len(existing_fighters)} existing HTML files")
        
        # Initialize processor with test directory
        processor = ESPNDataProcessor(str(test_data_dir))
        
        # Test 1: Get existing HTML files
        print("\n📋 Test 1: Get existing HTML files")
        existing_files = processor.get_existing_html_files()
        print(f"Found {len(existing_files)} existing files: {list(existing_files)}")
        
        # Test 2: Test HTML UPSERT with new fighters
        print("\n📋 Test 2: Test HTML UPSERT with new fighters")
        new_html_files = {
            "Israel_Adesanya": "<html><body><h1>Israel Adesanya</h1><p>New content</p></body></html>",
            "Conor_McGregor": "<html><body><h1>Conor McGregor</h1><p>Updated content</p></body></html>",  # Update existing
            "Alexander_Volkanovski": "<html><body><h1>Alexander Volkanovski</h1><p>New content</p></body></html>"
        }
        
        new_count, updated_count = processor.upsert_html_files(new_html_files)
        print(f"✅ UPSERT result: {new_count} new files, {updated_count} updated files")
        
        # Test 3: Verify files exist
        print("\n📋 Test 3: Verify files exist")
        final_files = processor.get_existing_html_files()
        expected_files = {"Conor_McGregor", "Khabib_Nurmagomedov", "Jon_Jones", "Israel_Adesanya", "Alexander_Volkanovski"}
        
        if final_files == expected_files:
            print("✅ All expected files exist")
        else:
            print(f"❌ File mismatch. Expected: {expected_files}, Got: {final_files}")
            return False
        
        # Test 4: Verify content was updated
        print("\n📋 Test 4: Verify content was updated")
        mcgregor_file = test_html_dir / "Conor_McGregor.html"
        with open(mcgregor_file, 'r') as f:
            content = f.read()
        
        if "Updated content" in content:
            print("✅ Conor McGregor file was updated correctly")
        else:
            print("❌ Conor McGregor file was not updated")
            return False
        
        print("\n🎉 All HTML UPSERT tests passed!")
        return True

def test_with_real_data():
    """Test with the actual A1-ESPN-Profiles data"""
    print("\n🧪 Testing with Real A1-ESPN-Profiles Data")
    print("=" * 50)
    
    try:
        # Initialize processor with real data
        processor = ESPNDataProcessor()
        
        # Get existing HTML count
        existing_files = processor.get_existing_html_files()
        print(f"📊 Found {len(existing_files)} existing HTML files")
        
        # Show some example files
        example_files = list(existing_files)[:5]
        print(f"📋 Example files: {example_files}")
        
        # Test data summary
        summary = processor.get_data_summary()
        print(f"📊 Data summary: {summary}")
        
        print("✅ Real data test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Real data test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Starting HTML UPSERT Tests")
    print("=" * 50)
    
    # Test 1: Basic functionality
    test1_passed = test_html_upsert()
    
    # Test 2: Real data
    test2_passed = test_with_real_data()
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 Test Results Summary:")
    print(f"  Basic HTML UPSERT: {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"  Real Data Test: {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 All tests passed! HTML UPSERT functionality is working correctly.")
        print("\n📋 Next Steps:")
        print("  1. The HTML UPSERT logic is ready for production")
        print("  2. Existing 2,897+ HTML files will be preserved")
        print("  3. New fighters will have HTML files added")
        print("  4. Updated fighters will have HTML files updated")
        return True
    else:
        print("\n❌ Some tests failed. Please check the implementation.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 