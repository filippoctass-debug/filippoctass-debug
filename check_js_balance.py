import sys

path = sys.argv[1]
s = open(path, "r", encoding="utf-8", errors="replace").read()

# stati stringhe
in_s = False   # '
in_d = False   # "
in_b = False   # `
esc = False

# stack parentesi (solo fuori dalle stringhe)
stack = []

line = 1
col = 0

def push(ch, line, col):
    stack.append((ch, line, col))

def pop(expected):
    if not stack: 
        return False
    top, l, c = stack[-1]
    pairs = {')':'(', ']':'[', '}':'{'}
    if top != pairs[expected]:
        return False
    stack.pop()
    return True

for i,ch in enumerate(s):
    if ch == "\n":
        line += 1
        col = 0
        continue
    col += 1

    if esc:
        esc = False
        continue

    # escape (solo dentro stringhe)
    if ch == "\\" and (in_s or in_d or in_b):
        esc = True
        continue

    # toggle stringhe
    if not (in_d or in_b) and ch == "'":
        in_s = not in_s
        continue
    if not (in_s or in_b) and ch == '"':
        in_d = not in_d
        continue
    if not (in_s or in_d) and ch == "`":
        in_b = not in_b
        continue

    # se siamo dentro una stringa, ignora parentesi
    if in_s or in_d or in_b:
        continue

    # parentesi/brackets/braces
    if ch in "([{":
        push(ch, line, col)
    elif ch in ")]}":
        if not pop(ch):
            print(f"ERRORE: chiusura '{ch}' inattesa a riga {line}, col {col}. Stack top={stack[-1] if stack else None}")
            sys.exit(2)

if in_s or in_d or in_b:
    kind = "'" if in_s else '"' if in_d else "`"
    print(f"ERRORE: stringa/template non chiusa ({kind}).")
    sys.exit(3)

if stack:
    ch, l, c = stack[-1]
    print(f"ERRORE: parentesi aperta non chiusa '{ch}' (aperta a riga {l}, col {c}).")
    sys.exit(4)

print("OK: nessun mismatch di base su virgolette/backtick/parentesi.")
