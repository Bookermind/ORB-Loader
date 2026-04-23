# Regular Expressions (Regex) Quick Reference

A practical guide to understanding regex patterns used in this project.

## What is Regex?

Regular expressions (regex) are patterns used to match text. They're powerful tools for finding, validating, and extracting information from strings like filenames, log entries, or data fields.

## Basic Building Blocks

### Literal Characters
Match exact characters:
- `abc` matches the text "abc"
- `file.csv` matches "file.csv" literally

### Special Characters (Metacharacters)
These have special meanings and need escaping with `\` to match literally:

| Character | Meaning              | Example                          |
|-----------|----------------------|----------------------------------|
| `.`       | Any single character | `a.c` matches "abc", "a9c", "a c" |
| `\`       | Escape character     | `\.` matches a literal period    |
| `^`       | Start of string      | `^sapar` must start with "sapar" |
| `$`       | End of string        | `\.csv$` must end with ".csv"    |
| `*`       | 0 or more of previous| `ab*c` matches "ac", "abc", "abbc" |
| `+`       | 1 or more of previous| `ab+c` matches "abc", "abbc" (not "ac") |
| `?`       | 0 or 1 of previous   | `colou?r` matches "color" or "colour" |
| `\|`      | OR operator          | `cat\|dog` matches "cat" or "dog" |

## Character Classes

### Predefined Classes
| Pattern | Matches              | Example                        |
|---------|----------------------|--------------------------------|
| `\d`    | Any digit (0-9)      | `\d\d` matches "42", "99"      |
| `\D`    | Any non-digit        | `\D+` matches "abc"            |
| `\w`    | Word character (a-z, A-Z, 0-9, _) | `\w+` matches "file_123" |
| `\s`    | Whitespace (space, tab, newline) | `\s+` matches "   " |
| `\S`    | Non-whitespace       | `\S+` matches "text"           |

### Custom Classes
Define your own character sets using `[]`:
- `[abc]` matches "a", "b", or "c"
- `[a-z]` matches any lowercase letter
- `[A-Z]` matches any uppercase letter
- `[0-9]` matches any digit (same as `\d`)
- `[^abc]` matches anything EXCEPT "a", "b", or "c" (the `^` inside `[]` means NOT)

## Quantifiers

Specify how many times a pattern should repeat:

| Quantifier | Meaning                | Example                          |
|------------|------------------------|----------------------------------|
| `{n}`      | Exactly n times        | `\d{4}` matches "2026"           |
| `{n,}`     | n or more times        | `\d{2,}` matches "42", "999"     |
| `{n,m}`    | Between n and m times  | `\d{2,4}` matches "42", "999", "2026" |
| `*`        | 0 or more (same as {0,}) | `a*` matches "", "a", "aaa"    |
| `+`        | 1 or more (same as {1,}) | `a+` matches "a", "aaa"        |
| `?`        | 0 or 1 (same as {0,1})   | `colou?r` matches "color", "colour" |

## Grouping and Capturing

### Parentheses `()`
Group patterns together and capture matches:
- `(abc)+` matches "abc", "abcabc", "abcabcabc"
- `(\d{8})` captures an 8-digit date like "20260410"

### Non-Capturing Groups `(?:)`
Group without capturing (more efficient when you don't need the match):
- `(?:abc)+` matches "abc", "abcabc" but doesn't store it

### Lookahead Assertions
Match based on what follows WITHOUT consuming characters:

| Pattern       | Meaning                           | Example                          |
|---------------|-----------------------------------|----------------------------------|
| `(?=...)`     | Positive lookahead (must follow)  | `\d(?=px)` matches "5" in "5px"  |
| `(?!...)`     | Negative lookahead (must NOT follow) | `\d(?!px)` matches "5" in "5em" |

## Practical Examples from This Project

### File Pattern Matching

```yaml
# Match data files (not trigger files)
filename_pattern: 'sapar_\d{8}\.csv$'
```
- `sapar_` - literal text "sapar_"
- `\d{8}` - exactly 8 digits (yyyymmdd)
- `\.` - literal period (escaped)
- `csv` - literal text "csv"
- `$` - end of string
- ✅ Matches: `sapar_20260410.csv`
- ❌ NOT: `sapar_20260410.trigger.csv`

---

```yaml
# Match trigger/companion files
pattern: 'sapar_.*\.trigger\.csv'
```
- `sapar_` - literal "sapar_"
- `.*` - any characters (0 or more)
- `\.trigger\.csv` - literal ".trigger.csv"
- ✅ Matches: `sapar_20260410.trigger.csv`

---

```yaml
# Extract date from filename
key_pattern: '(\d{8})'
```
- `(\d{8})` - captures exactly 8 digits
- Extracts: "20260410" from "sapar_20260410.csv"

---

```yaml
# Extract count from validation file
count_pattern: 'Count:\s*(\d+)'
```
- `Count:` - literal text
- `\s*` - optional whitespace (0 or more)
- `(\d+)` - captures 1 or more digits
- Matches: "Count: 42" or "Count:42", captures "42"

---

```yaml
# Extract amount (flexible currency)
amount_pattern: 'Amount:\s*([\$£€]?[\d,]+\.?\d*)'
```
- `Amount:\s*` - "Amount:" + optional whitespace
- `[\$£€]?` - optional currency symbol ($, £, or €)
- `[\d,]+` - digits and commas (one or more)
- `\.?` - optional decimal point
- `\d*` - optional decimal digits
- Matches: "Amount: $1,234.56" or "Amount:1234" or "Amount: £999.99"

## Common Pitfalls

### 1. **Forgetting to Escape Special Characters**
- ❌ `file.csv` matches "fileXcsv"
- ✅ `file\.csv` matches only "file.csv"

### 2. **Greedy vs Non-Greedy**
- `.*` is greedy (matches as much as possible)
- `.*?` is non-greedy (matches as little as possible)
- Example: In "abc123xyz", `a.*z` matches the whole string, but `a.*?z` would need more context

### 3. **Not Anchoring Patterns**
- ❌ `\d{8}\.csv` matches "abc20260410.csv" and "20260410.csv_backup"
- ✅ `^\d{8}\.csv$` matches only "20260410.csv"

### 4. **Case Sensitivity**
Most regex is case-sensitive by default:
- `abc` does NOT match "ABC"
- Use flags or character classes: `[Aa]bc` or enable case-insensitive mode

## Testing Your Regex

### Online Tools
- [regex101.com](https://regex101.com) - Interactive with explanations
- [regexr.com](https://regexr.com) - Visual highlighting
- [pythex.org](https://pythex.org) - Python-specific

### Python Testing
```python
import re

pattern = r'sapar_\d{8}\.csv$'
test_files = [
    'sapar_20260410.csv',           # Should match
    'sapar_20260410.trigger.csv',   # Should NOT match
]

for filename in test_files:
    match = re.match(pattern, filename)
    print(f"{filename}: {'✅ MATCH' if match else '❌ NO MATCH'}")
```

## Quick Reference Card

| Need to match...           | Pattern      | Example match    |
|---------------------------|--------------|------------------|
| Specific date format (yyyymmdd) | `\d{8}`      | "20260410"       |
| File extension            | `\.csv$`     | ".csv" at end    |
| Optional element          | `colou?r`    | "color" or "colour" |
| One of several options    | `cat\|dog`   | "cat" or "dog"   |
| Alphanumeric identifier   | `\w+`        | "file_123"       |
| Extract a value           | `(\d+)`      | Captures digits  |
| Start of line             | `^pattern`   | Must be at start |
| End of line               | `pattern$`   | Must be at end   |
| Exclude a pattern         | `(?!pattern)` | Negative lookahead |

## Further Reading

- [Python `re` module documentation](https://docs.python.org/3/library/re.html)
- [Regular-Expressions.info](https://www.regular-expressions.info/) - Comprehensive tutorials
- [MDN Regex Guide](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Regular_Expressions)
