import re
from typing import Dict, Tuple

# Mapping from size group label to pot size used in EV calculation
SIZE_MAP: Dict[str, float] = {
    "0-29%": 0.29,
    "30-45%": 0.45,
    "46-56%": 0.56,
    "57-70%": 0.70,
    "101%+": 1.50,
}

SIZE_KEYS = list(SIZE_MAP.keys())

PERCENT_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)%")


def load_summary_stats(path: str) -> Dict[str, Dict[str, str]]:
    """Load player stats from estatisticas_resumidas.html without external libs."""
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    players: Dict[str, Dict[str, str]] = {}
    start_marker = "<div class='player-summary'"
    idx = html.find(start_marker)
    line_re = re.compile(r"<div class='stat-line[^']*'[^>]*>([^:]+):\s*([^<]+)</div>")
    while idx != -1:
        next_idx = html.find(start_marker, idx + 1)
        block = html[idx:next_idx] if next_idx != -1 else html[idx:]
        name_m = re.search(r"<h2>([^<]+)</h2>", block)
        if name_m:
            name = name_m.group(1).strip()
            stats: Dict[str, str] = {}
            for stat, val in line_re.findall(block):
                stats[stat.strip().lower()] = val.strip()
            players[name] = stats
        if next_idx == -1:
            break
        idx = next_idx
    return players


def parse_percentage(value: str) -> float:
    """Extract numeric percentage from a display string."""
    m = PERCENT_RE.search(value)
    return float(m.group(1)) if m else 0.0


def compute_optimal_fold_size(stats: Dict[str, str], prefix: str) -> Tuple[str, float]:
    """Compute optimal bet size when opponent folds given size-specific stats."""
    best_size = None
    best_ev = float("-inf")
    for size_key in SIZE_KEYS:
        key = f"{prefix} {size_key}".lower()
        if key not in stats:
            continue
        pct = parse_percentage(stats[key]) / 100.0
        ev = pct - (1 - pct) * SIZE_MAP[size_key]
        if ev > best_ev:
            best_ev = ev
            best_size = size_key
    return best_size or "N/A", best_ev


def compute_optimal_bluff_size(stats: Dict[str, str], line_type: str) -> Tuple[str, float]:
    """Compute optimal size for bluffing on a given river line."""
    best_size = None
    best_ev = float("-inf")
    for size_key in SIZE_KEYS:
        air_key = f"{line_type} {size_key} air".lower()
        bluff_key = f"{line_type} {size_key} bluff vs mdf".lower()
        if air_key not in stats or bluff_key not in stats:
            continue
        air_pct = parse_percentage(stats[air_key]) / 100.0
        bluff_pct = parse_percentage(stats[bluff_key]) / 100.0
        ev = air_pct - (1 - bluff_pct) * SIZE_MAP[size_key]
        if ev > best_ev:
            best_ev = ev
            best_size = size_key
    return best_size or "N/A", best_ev


def get_bluff_classifications(stats: Dict[str, str], line_type: str) -> list[str]:
    """Return classification strings for bluffing frequencies of a line type."""
    results = []
    for size_key in SIZE_KEYS:
        key = f"{line_type} {size_key} bluff vs mdf".lower()
        if key in stats:
            results.append(f"{line_type} {size_key}: {stats[key]}")
    return results


def get_size_stat_lines(stats: Dict[str, str], prefix: str) -> list[str]:
    """Return lines for each size group for a given stat prefix."""
    results = []
    for size_key in SIZE_KEYS:
        key = f"{prefix.lower()} {size_key} (%)"
        if key in stats:
            results.append(f"{prefix} {size_key} (%): {stats[key]}")
    return results


def main(
    summary_path: str = "estatisticas_resumidas.html",
    output_path: str = "tamanhos_otimos.txt",
    html_path: str = "tamanhos_otimos.html",
) -> None:
    """Print and save optimal bet size information and HTML with search."""
    players = load_summary_stats(summary_path)
    lines = []
    results = {}
    for player, stats in players.items():
        player_lines = []
        size, ev = compute_optimal_fold_size(stats, "fold donk flop")
        player_lines.append(f"Fold Donk Flop Optimal Size: {size} (EV {ev:.3f})")

        size, ev = compute_optimal_fold_size(stats, "fold donk turn")
        player_lines.append(f"Fold Donk Turn Optimal Size: {size} (EV {ev:.3f})")

        size, ev = compute_optimal_fold_size(stats, "fold donk river")
        player_lines.append(f"Fold Donk River Optimal Size: {size} (EV {ev:.3f})")

        size, ev = compute_optimal_fold_size(stats, "fold cbet flop ip")
        player_lines.append(f"Fold CBet Flop IP Optimal Size: {size} (EV {ev:.3f})")

        size, ev = compute_optimal_fold_size(stats, "fold cbet turn")
        player_lines.append(f"Fold CBet Turn Optimal Size: {size} (EV {ev:.3f})")

        size, ev = compute_optimal_fold_size(stats, "fold cbet river")
        player_lines.append(f"Fold CBet River Optimal Size: {size} (EV {ev:.3f})")

        size, ev = compute_optimal_fold_size(stats, "fold probe turn")
        player_lines.append(f"Fold Probe Turn Optimal Size: {size} (EV {ev:.3f})")

        size, ev = compute_optimal_fold_size(stats, "fold probe river")
        player_lines.append(f"Fold Probe River Optimal Size: {size} (EV {ev:.3f})")

        # Include classification lines from summary for Fold CBet Turn and Fold Donk Turn
        for line in get_size_stat_lines(stats, "Fold CBet Turn"):
            player_lines.append(line)
        for line in get_size_stat_lines(stats, "Fold Donk Turn"):
            player_lines.append(line)

        for lt in ["BXB", "XBB", "BBB", "XXB"]:
            for class_line in get_bluff_classifications(stats, lt):
                player_lines.append(class_line)
        results[player] = player_lines
        lines.append(f"== {player} ==")
        lines.extend(player_lines)
        lines.append("")

    for line in lines:
        print(line)

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"Resultados salvos em '{output_path}'.")
    except Exception as e:
        print(f"Erro ao salvar '{output_path}': {e}")

    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write("<!DOCTYPE html>\n<html lang='pt-br'>\n<head>\n<meta charset='UTF-8'>\n")
            f.write("<title>Helper Resultados</title>\n")
            f.write("<style>body{font-family:sans-serif;background:#2c3e50;color:#ecf0f1;margin:10px;}"\
                    "#search{width:50%;padding:8px;margin-bottom:10px;border:1px solid #7f8c8d;border-radius:4px;background:#34495e;color:#ecf0f1;}"\
                    ".player{border:1px solid #7f8c8d;border-radius:5px;margin-bottom:10px;padding:10px;background:#34495e;display:none;}"\
                    ".player h2{text-align:center;margin-top:0;color:#3498db;}</style>\n")
            f.write("<script>function search(){var i,input=document.getElementById('search').value.toUpperCase();"\
                    "var divs=document.getElementsByClassName('player');for(i=0;i<divs.length;i++){var h=divs[i].getElementsByTagName('h2')[0];"\
                    "if(h&&h.innerText.toUpperCase().indexOf(input)>-1){divs[i].style.display='block';}else{divs[i].style.display='none';}}"\
                    "if(input===''){for(i=0;i<divs.length;i++){divs[i].style.display='none';}}}</script>\n")
            f.write("</head><body>\n<h1>Tamanhos Ótimos</h1>\n<input id='search' onkeyup='search()' placeholder='Buscar jogador...'>\n")
            for player, pls in results.items():
                f.write(f"<div class='player'><h2>{player}</h2>\n")
                for line in pls:
                    f.write(f"<div>{line}</div>\n")
                f.write("</div>\n")
            f.write("<script>document.addEventListener('DOMContentLoaded',search);</script></body></html>")
        print(f"HTML salvo em '{html_path}'.")
    except Exception as e:
        print(f"Erro ao salvar HTML '{html_path}': {e}")


if __name__ == "__main__":
    main()