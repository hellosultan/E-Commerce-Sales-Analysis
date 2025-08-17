from pathlib import Path
def first_writable(paths):
    for p in paths:
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            (p.parent / ".write_test").write_text("ok")
            (p.parent / ".write_test").unlink(missing_ok=True)
            return p
        except Exception:
            continue
    return None
