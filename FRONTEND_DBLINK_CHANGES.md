# Frontend DBLink Support - Implementation Summary

## Overview
Updated the v2 portal frontend to allow users to input Oracle table names with DBLink notation when creating datasets.

## Changes Made

### 1. HTML Changes (`public/v2.html`)

**Line 279**: Added unique ID to help text element
```html
<small class="v2-help" id="wizardDatasetNameHelp">Use apenas letras, números e underscore</small>
```

This enables dynamic updates of the help text based on selected source type.

### 2. JavaScript Changes (`public/v2.js`)

#### A. Dynamic Help Text (Lines 583-598)
Added event listener to change help text and placeholder when source type changes:

```javascript
// Source type change - update dataset name help text for Oracle DBLink
document.querySelectorAll('input[name="wizardSourceType"]').forEach(radio => {
  radio.addEventListener('change', (e) => {
    const sourceType = e.target.value;
    const helpText = document.getElementById('wizardDatasetNameHelp');
    const inputField = document.getElementById('wizardDatasetName');
    
    if (sourceType === 'ORACLE') {
      helpText.textContent = 'Para tabelas com DBLink use: SCHEMA.TABELA@DBLINK (ex: CMASTER.CMALU@CMASTERPRD)';
      inputField.placeholder = 'ex: CMASTER.CMALUINTERNO@CMASTERPRD';
    } else {
      helpText.textContent = 'Use apenas letras, números e underscore';
      inputField.placeholder = 'ex: glo_agentes';
    }
  });
});
```

#### B. Updated Validation Logic (Lines 654-676)
Modified the validation in Step 2 to accept different formats based on source type:

```javascript
case 2:
  const connection = document.getElementById('wizardConnection').value;
  const datasetName = document.getElementById('wizardDatasetName').value.trim();
  const sourceType = document.querySelector('input[name="wizardSourceType"]:checked').value;
  
  if (!connection || !datasetName) {
    showToast('Preencha conexão e nome do dataset', 'warning');
    return false;
  }
  
  // Different validation rules for Oracle vs other sources
  if (sourceType === 'ORACLE') {
    // Oracle allows: SCHEMA.TABLE@DBLINK format
    if (!/^[A-Za-z0-9_@.]+$/.test(datasetName)) {
      showToast('Nome do dataset inválido para ORACLE (use: SCHEMA.TABELA@DBLINK ou TABELA)', 'warning');
      return false;
    }
  } else {
    // Other sources: only lowercase alphanumeric and underscore
    if (!/^[a-z0-9_]+$/.test(datasetName)) {
      showToast('Nome do dataset deve conter apenas letras minúsculas, números e underscore', 'warning');
      return false;
    }
  }
  return true;
```

**Key changes:**
- Gets the selected source type before validation
- For Oracle: accepts uppercase, lowercase, numbers, underscore, `@`, and `.` characters
- For other sources: maintains original validation (lowercase alphanumeric and underscore only)

#### C. Initial State Setup (Lines 797-803)
Updated `resetWizard()` to set Oracle help text by default:

```javascript
// Set initial help text for Oracle (default source type)
const helpText = document.getElementById('wizardDatasetNameHelp');
const inputField = document.getElementById('wizardDatasetName');
if (helpText && inputField) {
  helpText.textContent = 'Para tabelas com DBLink use: SCHEMA.TABELA@DBLINK (ex: CMASTER.CMALU@CMASTERPRD)';
  inputField.placeholder = 'ex: CMASTER.CMALUINTERNO@CMASTERPRD';
}
```

This ensures the correct help text appears when the wizard is first opened (Oracle is the default source type).

## User Experience

### Before
- Users could only enter simple table names like `glo_agentes`
- No guidance about DBLink format
- Validation rejected `@` and `.` characters
- DBLink tables had to be manually corrected in the database

### After
- **For Oracle (default):**
  - Help text: "Para tabelas com DBLink use: SCHEMA.TABELA@DBLINK (ex: CMASTER.CMALU@CMASTERPRD)"
  - Placeholder: "ex: CMASTER.CMALUINTERNO@CMASTERPRD"
  - Accepts: `SCHEMA.TABLE@DBLINK` format
  
- **For SharePoint:**
  - Help text: "Use apenas letras, números e underscore"
  - Placeholder: "ex: glo_agentes"
  - Accepts: Only `[a-z0-9_]` format

### Valid Examples
- `CMASTER.CMALUINTERNO@CMASTERPRD` ✓
- `SCHEMA.TABLE@DBLINK` ✓
- `SIMPLETABLE` ✓
- `myschema.mytable` ✓

## Testing

A verification script was created at `scripts/verify_frontend_dblink.py` that confirms:
1. ✓ HTML has unique ID for dynamic help text
2. ✓ JavaScript has source type change listener
3. ✓ Dynamic help text is implemented
4. ✓ Oracle placeholder example is set
5. ✓ Oracle validation regex accepts `@` and `.`
6. ✓ Initial help text is set for Oracle

All checks passed.

## Integration with Backend

These frontend changes work with the backend modifications made in:
- `src/portalRoutes.js` (lines 947-968): Backend validation accepts `[A-Za-z0-9_@.]+` for Oracle
- `databricks_notebooks/governed_ingestion_orchestrator.py`: Bypasses validation for DBLink tables

## Next Steps

1. Deploy the updated frontend files to the web server
2. Test creating a new dataset via the portal with DBLink notation
3. Verify the dataset can be executed successfully
4. Monitor logs for the expected DBLink bypass messages
