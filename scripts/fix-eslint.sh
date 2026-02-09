#!/bin/bash

# ESLint Error Fix Script for PIVOT_QUANT
# This script applies all specified ESLint fixes

echo "🔧 ESLint Error Fix Script for PIVOT_QUANT"
echo "=========================================="

# Make the JavaScript fixer executable
chmod +x fix-eslint-errors.js

echo "📁 Checking for target files..."

# List all files that need to be fixed
files_to_check=(
    "src/config/ConfigurationManager.js"
    "src/core/QuantPivotEngine.js"
    "src/index.js"
    "src/math/MathematicalModels.js"
    "src/monitoring/PerformanceMonitor.js"
    "src/validation/ValidationFramework.js"
    "tests/QuantPivotEngine.test.js"
)

found_files=0
missing_files=()

for file in "${files_to_check[@]}"; do
    if [ -f "$file" ]; then
        echo "✅ Found: $file"
        ((found_files++))
    else
        echo "❌ Missing: $file"
        missing_files+=("$file")
    fi
done

echo ""
echo "📊 Summary: $found_files files found, ${#missing_files[@]} files missing"

if [ $found_files -eq 0 ]; then
    echo ""
    echo "⚠️  No target files found in expected locations."
    echo "   The files may be in a different directory structure."
    echo ""
    echo "🔍 Searching for files in current directory..."

    # Search for files with similar names
    find . -name "*.js" -type f | grep -E "(Configuration|QuantPivot|Mathematical|Validation|Performance)" || echo "   No matching files found."

    echo ""
    echo "💡 To use this script:"
    echo "   1. Ensure the files exist in the expected src/ and tests/ directories"
    echo "   2. Or modify the paths in fix-eslint-errors.js to match your structure"
    echo "   3. Run: node fix-eslint-errors.js"
else
    echo ""
    echo "🚀 Running ESLint fixes..."
    node fix-eslint-errors.js

    echo ""
    echo "🧪 Running ESLint to verify fixes..."
    if command -v npx &> /dev/null; then
        if npx eslint "${files_to_check[@]}" 2>/dev/null; then
            echo "✅ All ESLint errors have been fixed!"
        else
            echo "⚠️  Some ESLint issues may remain. Run 'npx eslint .' to check."
        fi
    else
        echo "ℹ️  ESLint not found. Install with: npm install eslint"
    fi
fi

echo ""
echo "✅ Script completed!"