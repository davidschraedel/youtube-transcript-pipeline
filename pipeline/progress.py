"""Plain-text progress lines for terminal runs (stdout)."""


def section(title: str) -> None:
    print(f"\n--- {title} ---", flush=True)


def line(text: str) -> None:
    print(f"  {text}", flush=True)


def skip(title: str) -> None:
    section(title)
    line("(skipped — nothing to do)")
