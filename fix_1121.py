import sys, re
p = sys.argv[1]
s = open(p, "r", encoding="utf-8").read()

# sostituisce SOLO quella chiamata pushEvent con versione senza template literal
s2, n = re.subn(
    r'pushEvent\("Serie",\s*`Impossibile caricare serie PV: \$\{e\.message\}`, "warn"\);',
    'pushEvent("Serie", "Impossibile caricare serie PV: " + ((e && e.message) ? e.message : e), "warn");',
    s
)

if n == 0:
    print("WARN: pattern non trovato (forse la riga è diversa).")
else:
    open(p, "w", encoding="utf-8").write(s2)
    print("OK: sostituita la pushEvent Serie (no backticks).")
