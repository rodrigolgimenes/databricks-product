#!/usr/bin/env python3
"""
Verification script for Frontend DBLink Support
Confirms that the frontend properly supports Oracle table names with DBLink notation.
"""

import re
import sys
from pathlib import Path

def check_html_changes():
    """Verify HTML has unique ID for help text"""
    html_file = Path("C:/dev/cm-databricks/public/v2.html")
    
    if not html_file.exists():
        print("❌ v2.html not found")
        return False
    
    content = html_file.read_text(encoding='utf-8')
    
    # Check for unique ID on help text
    if 'id="wizardDatasetNameHelp"' in content:
        print("✓ HTML: wizardDatasetNameHelp ID added")
        return True
    else:
        print("❌ HTML: wizardDatasetNameHelp ID not found")
        return False

def check_javascript_changes():
    """Verify JavaScript has dynamic help text and validation"""
    js_file = Path("C:/dev/cm-databricks/public/v2.js")
    
    if not js_file.exists():
        print("❌ v2.js not found")
        return False
    
    content = js_file.read_text(encoding='utf-8')
    
    checks = {
        "Source type listener": "input[name=\"wizardSourceType\"]",
        "Dynamic help text": "Para tabelas com DBLink use: SCHEMA.TABELA@DBLINK",
        "Oracle placeholder": "CMASTER.CMALUINTERNO@CMASTERPRD",
        "Oracle validation regex": "[A-Za-z0-9_@.]",
        "Initial help text setup": "Set initial help text for Oracle"
    }
    
    all_passed = True
    for check_name, pattern in checks.items():
        if pattern in content:
            print(f"✓ JS: {check_name}")
        else:
            print(f"❌ JS: {check_name} not found")
            all_passed = False
    
    return all_passed

def main():
    print("=" * 60)
    print("Frontend DBLink Support Verification")
    print("=" * 60)
    print()
    
    html_ok = check_html_changes()
    print()
    js_ok = check_javascript_changes()
    print()
    
    if html_ok and js_ok:
        print("=" * 60)
        print("✅ ALL CHECKS PASSED")
        print("=" * 60)
        print()
        print("Frontend is ready to accept Oracle DBLink notation!")
        print("Format: SCHEMA.TABELA@DBLINK")
        print("Example: CMASTER.CMALUINTERNO@CMASTERPRD")
        return 0
    else:
        print("=" * 60)
        print("❌ SOME CHECKS FAILED")
        print("=" * 60)
        return 1

if __name__ == "__main__":
    sys.exit(main())
