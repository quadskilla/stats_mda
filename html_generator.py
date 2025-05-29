# html_generator.py
import math
# Não precisa importar PlayerStats se a classe PlayerStats já tem os métodos de display
# e to_dict_display, e get_raw_stat_value. Se esses métodos fossem movidos para cá,
# então precisaria importar a classe.
# from stats_calculator import PlayerStats, PF_POS_CATS_FOR_STATS, PF_POS_CATS_FOR_CALL_STATS, _classify_percentage, BLUFF_CLASS_THRESHOLDS, FOLD_CLASS_THRESHOLDS, STAT_COLOR_RANGES

# As constantes usadas na geração de HTML precisam estar acessíveis aqui.
# Melhor mantê-las em stats_calculator.py e importá-las se necessário, ou passá-las.
# Por simplicidade, vou assumir que PlayerStats tem os métodos necessários.

# Se STAT_COLOR_RANGES, _classify_percentage, etc., são usados APENAS aqui,
# podem ser movidos para cá. Caso contrário, importe-os.
# Para manter stats_calculator.py focado, vamos assumir que get_stat_color_class
# e a lógica de stat_block_structure são parte da geração de HTML.

STAT_COLOR_RANGES = {
    "VPIP (%)": [{"max": 18, "class": "stat-tight"}, {"max": 28, "class": "stat-normal"}, {"max": 100, "class": "stat-loose"}],
    "PFR (%)": [{"max": 12, "class": "stat-passive"}, {"max": 20, "class": "stat-std-agg"}, {"max": 100, "class": "stat-very-agg"}],
    # ... (resto do STAT_COLOR_RANGES) ...
}
# Copie as constantes BLUFF_CLASS_THRESHOLDS, FOLD_CLASS_THRESHOLDS de stats_calculator.py se forem usadas diretamente aqui
# E a função _classify_percentage
# Ou importe-as: from stats_calculator import _classify_percentage, BLUFF_CLASS_THRESHOLDS, FOLD_CLASS_THRESHOLDS (e outras necessárias)


# Temporariamente, vamos assumir que PlayerStats tem get_raw_stat_value
# e que as constantes de threshold são acessadas através de PlayerStats ou globalmente.
# Para uma separação mais limpa, _classify_percentage e as THRESHOLDS poderiam estar em um utils.py
# ou serem passadas como argumento para get_stat_color_class.

# Esta função é usada por get_stat_color_class. Se get_stat_color_class estiver aqui, _classify_percentage também deve estar,
# junto com BLUFF_CLASS_THRESHOLDS e FOLD_CLASS_THRESHOLDS.
# from stats_calculator import _classify_percentage, BLUFF_CLASS_THRESHOLDS, FOLD_CLASS_THRESHOLDS (se elas ficarem lá)
# Por ora, vamos duplicar para manter este arquivo mais autônomo na geração do HTML:

BLUFF_CLASS_THRESHOLDS_HTML = { "0-29%": (18.5, 19.5), "30-45%": (23.68, 24.5), "46-56%": (26.41, 27.5), "57-70%": (29.1, 30.5), "71-100%": (33.0, 34.0), "101%+": (40.0, 41.0) }
FOLD_CLASS_THRESHOLDS_HTML = { "0-29%": (22.5, 23.5), "30-45%": (31.0, 32.0), "46-56%": (35.8, 36.9), "57-70%": (41.1, 42.2), "71-100%": (50.0, 51.0), "101%+": (60.0, 61.0) }

def _classify_percentage_html(size_group, pct, thresholds_dict):
    th = thresholds_dict.get(size_group)
    if not th or pct is None: return None
    under_max, gto_max = th
    if pct <= under_max: return "under"
    elif pct <= gto_max: return "gto"
    return "over"


def get_stat_color_class(stat_name, stat_value_numeric, player_stat_obj=None): # Adicionado player_stat_obj
    if stat_value_numeric is None or math.isnan(stat_value_numeric) or math.isinf(stat_value_numeric): return ""
    
    if stat_name.startswith("River ") and " Air (%)" in stat_name:
        parts = stat_name.split(" ")
        if len(parts) >= 4: 
            size_group = parts[2]
            label = _classify_percentage_html(size_group, stat_value_numeric, BLUFF_CLASS_THRESHOLDS_HTML)
            color_map = {"under": "stat-tight", "gto": "stat-high", "over": "stat-normal"} 
            return color_map.get(label, "")

    if stat_name.startswith("FTS ") or stat_name.startswith("Fold CBet ") or stat_name.startswith("Fold Donk ") or stat_name.startswith("CF Turn "):
        parts = stat_name.split(" ")
        size_group = None
        for part_idx, part in enumerate(parts):
            if "%" in part and "-" in part: 
                size_group = part
                break
            # Para CF Turn <size_group> (%), o size group é parts[2]
            elif stat_name.startswith("CF Turn ") and part_idx == 2 and "%" in part:
                 size_group = part
                 break

        if size_group:
            label = _classify_percentage_html(size_group.replace(" (%)",""), stat_value_numeric, FOLD_CLASS_THRESHOLDS_HTML)
            color_map = {"under": "stat-tight", "gto": "stat-high", "over": "stat-normal"} 
            return color_map.get(label, "")
            
    ranges = STAT_COLOR_RANGES.get(stat_name)
    if ranges:
        for r_color in ranges:
            if stat_value_numeric <= r_color["max"]: return r_color["class"]
    return ""


def generate_html_grid(stats_data, output_filename="estatisticas_poker_grid.html"):
    # Importar PlayerStats aqui se as constantes PF_POS_CATS forem necessárias e estiverem lá
    from stats_calculator import PF_POS_CATS_FOR_STATS, PF_POS_CATS_FOR_CALL_STATS

    print(f"Salvando estatísticas em HTML (Grid Layout) em '{output_filename}'...")
    try:
        with open(output_filename, "w", encoding="utf-8") as htmlfile:
            # ... (COPIE A LÓGICA DE GERAÇÃO DE HTML DO GRID AQUI)
            # ... (Ela usará o stats_data recebido)
            htmlfile.write("<!DOCTYPE html>\n<html lang='pt-br'>\n<head>\n  <meta charset='UTF-8'>\n")
            # ... (resto do HTML)
            htmlfile.write("  <title>Poker Stats Grid</title>\n")
            htmlfile.write("  <style>\n")
            htmlfile.write("    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 10px; background-color: #2c3e50; color: #ecf0f1; font-size: 13px; }\n")
            htmlfile.write("    .hud-container { display: flex; flex-direction: column; align-items: center; }\n")
            htmlfile.write("    h1 { text-align: center; color: #ecf0f1; margin-bottom: 15px; }\n")
            htmlfile.write("    input#searchInput { width: 50%; padding: 10px; margin-bottom: 15px; border: 1px solid #7f8c8d; border-radius: 4px; background-color: #34495e; color: #ecf0f1; font-size: 0.9em; }\n")
            htmlfile.write("    .player-hud { border: 1px solid #7f8c8d; border-radius: 5px; margin-bottom: 15px; padding: 10px; background-color: #34495e; width: 95%; max-width: 1200px; display: none; }\n") 
            htmlfile.write("    .player-hud h2 { margin-top: 0; border-bottom: 1px solid #7f8c8d; padding-bottom: 5px; color: #3498db; text-align: center; }\n")
            htmlfile.write("    .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 10px; }\n")
            htmlfile.write("    .stat-block { background-color: #4a627a; padding: 10px; border-radius: 4px; }\n")
            htmlfile.write("    .stat-block h3 { margin-top: 0; color: #95a5a6; font-size: 0.95em; border-bottom: 1px solid #566f88; padding-bottom: 4px;}\n")
            htmlfile.write("    .stat-item { display: flex; justify-content: space-between; margin-bottom: 4px; font-size: 0.85em;}\n")
            htmlfile.write("    .stat-label { color: #bdc3c7; flex-basis: 70%; }\n") 
            htmlfile.write("    .stat-value { color: #ecf0f1; font-weight: bold; text-align: right; flex-basis: 30%; }\n")
            htmlfile.write("    .stat-tight, .stat-passive, .stat-low { color: #e74c3c !important; } \n")
            htmlfile.write("    .stat-normal, .stat-std-agg, .stat-mid { color: #2ecc71 !important; } \n")
            htmlfile.write("    .stat-loose, .stat-very-agg, .stat-high { color: #3498db !important; } \n")
            htmlfile.write("  </style>\n</head>\n<body>\n  <div class='hud-container'>\n") 
            htmlfile.write("  <h1>Estatísticas de Poker - HUD View</h1>\n")
            htmlfile.write("  <input type='text' id='searchInput' onkeyup='searchPlayerHud()' placeholder='Buscar jogador para exibir HUD...'>\n")

            size_groups_display = ["0-29%", "30-45%", "46-56%", "57-70%", "80-100%", "101%+"] 
            line_types_display = ["BBB", "BXB", "XBB", "XXB"]
            hand_cat_display_map = {"topo": "Topo", "bluff_catcher": "BluffCatcher", "air": "Air"}

            stat_block_structure = {
                "Geral": ["Hands Played", "VPIP (%)", "PFR (%)"],
                "Pré-Flop Avançado": ["3Bet PF (%)", "Fold to PF 3Bet (%)", "Squeeze PF (%)", "4Bet PF (%)", "Fold to PF 4Bet (%)"],
                "PF vs Steal (BB)": ["Fold BB vs BTN Steal (%)", "Fold BB vs CO Steal (%)", "Fold BB vs SB Steal (%)"],
                "PF Open Raise": [f"OR {pos} (%)" for pos in PF_POS_CATS_FOR_STATS],
                "PF Call Open Raise": [f"Call OR {pos} (%)" for pos in PF_POS_CATS_FOR_CALL_STATS],
                "Flop": [
                    "CBet Flop (%)", "CBet Flop IP (%)", "CBet Flop OOP (%)", "Fold to Flop CBet (%)",
                    "Fold to Flop CBet IP (%)", "Fold to Flop CBet OOP (%)",
                    "Donk Bet Flop (%)", "Fold to Donk Flop (%)",
                    "Bet vs Missed CBet Flop (%)", "Fold to Bet vs Missed CBet Flop (%)",
                    "Check-Call Flop (%)", "Check-Fold Flop (%)", "Check-Raise Flop (%)", "Fold to XR Flop (%)",
                    "PFA SkipCB&XC Flop (%)", "PFA SkipCB&XF Flop (%)", "PFA SkipCB&XR Flop (%)"
                ],
                "Turn": [
                    "CBet Turn (%)", "Fold to Turn CBet (%)", "Fold to Turn CBet IP (%)", "Fold to Turn CBet OOP (%)",
                    "Donk Bet Turn (%)", "Fold to Donk Turn (%)",
                    "Probe Bet Turn (%)", "Fold to Probe Turn (%)",
                    "Bet vs Missed CBet Turn (%)", "Fold to Bet vs Missed CBet Turn (%)",
                    "Check-Call Turn (%)", "Check-Fold Turn (%)", "Check-Raise Turn (%)", "Fold to XR Turn (%)"
                ],
                "River": [
                    "CBet River (%)", "Fold to River CBet (%)","Fold to River CBet IP (%)", "Fold to River CBet OOP (%)", "Bet River (%)",
                    "Donk Bet River (%)", "Fold to Donk River (%)",
                    "Probe Bet River (%)", "Fold to Probe River (%)",
                    "Bet vs Missed CBet River (%)", "Fold to Bet vs Missed CBet River (%)",
                    "Check-Call River (%)", "Check-Fold River (%)", "Check-Raise River (%)", "Fold to XR River (%)"
                ],
                "FTS Flop": [f"FTS Flop {sg} (%)" for sg in size_groups_display],
                "FTS Turn": [f"FTS Turn {sg} (%)" for sg in size_groups_display],
                "FTS River": [f"FTS River {sg} (%)" for sg in size_groups_display],
                "Call-Fold Turn": [f"CF Turn {sg} (%)" for sg in size_groups_display], 
                "Fold CBet Flop IP": [f"Fold CBet Flop IP {sg} (%)" for sg in size_groups_display],
                "Fold CBet Flop OOP": [f"Fold CBet Flop OOP {sg} (%)" for sg in size_groups_display],
                "Fold Donk Flop": [f"Fold Donk Flop {sg} (%)" for sg in size_groups_display],
                "Fold Donk Turn": [f"Fold Donk Turn {sg} (%)" for sg in size_groups_display],
                "Fold Donk River": [f"Fold Donk River {sg} (%)" for sg in size_groups_display],
                "Extra River": ["CCF vs Triple Barrel (%)", "BBF vs Donk River (%)"],
            }
            for lt in line_types_display:
                stat_block_structure[f"FTS River {lt}"] = [f"FTS River {lt} {sg} (%)" for sg in size_groups_display]

            for lt in line_types_display:
                block_title_comp = f"River {lt} Composition"
                block_title_bv = f"River {lt} Bluff/Value"
                keys_comp = []
                keys_bv = []
                line_has_any_data_overall = False
                if stats_data: 
                    for p_stats_obj in stats_data.values():
                        line_data_check = p_stats_obj.river_bet_called_composition_by_line.get(lt)
                        if line_data_check:
                            for sg_data_dict_check in line_data_check.values():
                                if sg_data_dict_check.get('total_showdowns',0) > 0:
                                    line_has_any_data_overall = True; break
                            if line_has_any_data_overall: break
                
                if line_has_any_data_overall:
                    for sg in size_groups_display:
                        for hc_display in hand_cat_display_map.values():
                            keys_comp.append(f"River {lt} {sg} {hc_display} (%)")
                        keys_bv.append(f"River {lt} {sg} Bluff (%)")
                        keys_bv.append(f"River {lt} {sg} Value (%)")
                        keys_bv.append(f"River {lt} {sg} Bluff vs MDF")
                    if keys_comp : stat_block_structure[block_title_comp] = keys_comp
                    if keys_bv : stat_block_structure[block_title_bv] = keys_bv
            
            sorted_player_names_html = sorted(stats_data.keys())

            for player_name_html_main in sorted_player_names_html:
                if player_name_html_main not in stats_data: continue 
                player_stat_obj_html_main = stats_data[player_name_html_main]
                stat_dict_display_html_main = player_stat_obj_html_main.to_dict_display()

                htmlfile.write(f"  <div class='player-hud' id='hud-{player_name_html_main.replace(' ', '-').replace('.', '')}'>\n")
                htmlfile.write(f"    <h2>{player_name_html_main}</h2>\n")
                htmlfile.write("    <div class='stat-grid'>\n")

                for block_title_html_main, stat_keys_in_block_html_main in stat_block_structure.items():
                    active_stat_keys_for_player_in_block_html_main = []
                    for sk_html_main in stat_keys_in_block_html_main:
                        if sk_html_main in stat_dict_display_html_main:
                            val_display_html_main = str(stat_dict_display_html_main[sk_html_main])
                            always_show_block_type_html_main = block_title_html_main == "Geral" or \
                                                     block_title_html_main.startswith("PF") or \
                                                     block_title_html_main.startswith("FTS") or \
                                                     (block_title_html_main.startswith("River") and "Composition" in block_title_html_main) or \
                                                     (block_title_html_main.startswith("River") and "Bluff/Value" in block_title_html_main)
                            has_data_html_main = not (val_display_html_main.startswith("0.0%") and val_display_html_main.endswith("(0/0)"))
                            if (block_title_html_main.startswith("River ") and ("Composition" in block_title_html_main or "Bluff/Value" in block_title_html_main)) or \
                               always_show_block_type_html_main or has_data_html_main:
                                active_stat_keys_for_player_in_block_html_main.append(sk_html_main)
                    
                    if not active_stat_keys_for_player_in_block_html_main: continue

                    htmlfile.write("      <div class='stat-block'>\n")
                    htmlfile.write(f"        <h3>{block_title_html_main}</h3>\n")
                    for stat_key_html_main in active_stat_keys_for_player_in_block_html_main:
                        display_value_html_main = stat_dict_display_html_main.get(stat_key_html_main, "-")
                        # Passar o objeto player_stat_obj_html_main para get_raw_stat_value
                        numeric_val_for_color_html_main = player_stat_obj_html_main.get_raw_stat_value(stat_key_html_main)                         
                        color_class_html_main = get_stat_color_class(stat_key_html_main, 
                                                                     numeric_val_for_color_html_main if isinstance(numeric_val_for_color_html_main, (int, float)) else 0.0,
                                                                     player_stat_obj_html_main) # Passa o objeto
                        label_html_main = stat_key_html_main.replace(' (%)','').replace('FTS ','FTS ') 
                        if block_title_html_main.startswith("River") and ("Composition" in block_title_html_main or "Bluff/Value" in block_title_html_main) :
                            parts_label_main = stat_key_html_main.split(" ")
                            if len(parts_label_main) >= 4:
                                if parts_label_main[-1] == "(%)":
                                    label_html_main = f"{parts_label_main[-3]} {parts_label_main[-2]}" 
                                elif parts_label_main[-2] == "vs" and parts_label_main[-1] == "MDF":
                                     label_html_main = f"{parts_label_main[-4]} {parts_label_main[-3]} vs MDF" 
                                else:
                                     label_html_main = " ".join(parts_label_main[2:])
                        htmlfile.write(f"        <div class='stat-item'>\n")
                        htmlfile.write(f"          <span class='stat-label'>{label_html_main}</span>\n")
                        htmlfile.write(f"          <span class='stat-value {color_class_html_main}'>{display_value_html_main}</span>\n")
                        htmlfile.write("        </div>\n")
                    htmlfile.write("      </div>\n")
                htmlfile.write("    </div>\n") 
                htmlfile.write("  </div>\n") 

            htmlfile.write("""
  <script>
    function searchPlayerHud() {
      var input, filter, huds, i, hud_name_element, hud_name_text;
      input = document.getElementById("searchInput");
      filter = input.value.toUpperCase();
      huds = document.getElementsByClassName("player-hud");
      for (i = 0; i < huds.length; i++) {
        hud_name_element = huds[i].getElementsByTagName("h2")[0];
        if (hud_name_element) {
            hud_name_text = hud_name_element.textContent || hud_name_element.innerText;
            if (hud_name_text.toUpperCase().indexOf(filter) > -1) {
                huds[i].style.display = "block"; 
            } else {
                huds[i].style.display = "none"; 
            }
        }
      }
      if (filter === "") {
          for (i = 0; i < huds.length; i++) {
              huds[i].style.display = "none";
          }
      }
    }
    document.addEventListener('DOMContentLoaded', function() {
        var huds = document.getElementsByClassName("player-hud");
        for (var i = 0; i < huds.length; i++) {
            huds[i].style.display = "none";
        }
    });
  </script>
""")
            htmlfile.write("</div>\n</body>\n</html>")
        print(f"Estatísticas HTML (Grid) salvas com sucesso em '{output_filename}'.")
    except Exception as e:
        print(f"ERRO ao salvar o arquivo HTML (Grid) '{output_filename}': {e}")
        import traceback
        traceback.print_exc()


def generate_html_summary(stats_data, output_filename="estatisticas_resumidas.html"):
    print(f"Salvando resumo em HTML em '{output_filename}'...")
    try:
        with open(output_filename, "w", encoding="utf-8") as sf:
            # ... (COPIE A LÓGICA DE GERAÇÃO DE HTML DO RESUMO AQUI)
            # ... (Ela usará o stats_data recebido)
            sf.write("<!DOCTYPE html>\n<html lang='pt-br'>\n<head>\n  <meta charset='UTF-8'>\n")
            # ... (resto do HTML)
            sf.write("  <title>Estatísticas Resumidas</title>\n")
            sf.write("  <style>\n")
            sf.write("    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #2c3e50; color: #ecf0f1; margin: 10px; }\n")
            sf.write("    #searchInputResumo { width: 50%; padding: 10px; margin-bottom: 15px; border: 1px solid #7f8c8d; border-radius: 4px; background-color: #34495e; color: #ecf0f1; }\n")
            sf.write("    .player-summary { border: 1px solid #7f8c8d; border-radius: 5px; margin-bottom: 15px; padding: 10px; background-color: #34495e; display: none; }\n") 
            sf.write("    .player-summary h2 { margin-top: 0; color: #3498db; text-align: center; }\n")
            sf.write("    .stat-line { margin: 3px 0; }\n")
            sf.write("    .stat-tight { color: #e74c3c !important; }\n")
            sf.write("    .stat-high { color: #2ecc71 !important; }\n") 
            sf.write("    .stat-normal { color: #3498db !important; }\n") 
            sf.write("  </style>\n</head>\n<body>\n")
            sf.write("  <h1>Estatísticas Resumidas</h1>\n")
            sf.write("  <input type='text' id='searchInputResumo' onkeyup='searchResumo()' placeholder='Buscar jogador...'>\n")

            for player_name_sum_main in sorted(stats_data.keys()):
                stat_dict_display_sum_main = stats_data[player_name_sum_main].to_dict_display()
                sf.write(f"  <div class='player-summary' id='resumo-{player_name_sum_main.replace(' ', '-').replace('.', '')}'>\n")
                sf.write(f"    <h2>{player_name_sum_main}</h2>\n")
                for k_sum_main, v_sum_main in stat_dict_display_sum_main.items():
                    value_str_sum_main = str(v_sum_main)
                    upper_val_sum_main = value_str_sum_main.upper()
                    show_this_stat_in_summary_main = False
                    if any(lbl_sum_main in upper_val_sum_main for lbl_sum_main in ['UNDER', 'GTO', 'OVER']):
                        show_this_stat_in_summary_main = True
                    elif k_sum_main in ["Hands Played", "VPIP (%)", "PFR (%)", "3Bet PF (%)", "Fold to PF 3Bet (%)", "CBet Flop (%)", "Fold to Flop CBet (%)"]:
                        show_this_stat_in_summary_main = True
                    
                    if show_this_stat_in_summary_main:
                        label_sum_main = k_sum_main.replace('CF Turn', 'C/F Turn').replace('Bluff vs MDF', 'Bluff vs MDF')
                        label_sum_main = label_sum_main.replace('River ', '').replace(' (%)', '')                             
                        color_class_sum_main = ''
                        # Passar o objeto player_stat para get_stat_color_class
                        numeric_val_sum_color_main = stats_data[player_name_sum_main].get_raw_stat_value(k_sum_main)
                        if isinstance(numeric_val_sum_color_main, (int, float)):
                            color_class_sum_main = get_stat_color_class(k_sum_main, numeric_val_sum_color_main, stats_data[player_name_sum_main])
                        
                        if 'UNDER' in upper_val_sum_main: color_class_sum_main = 'stat-tight'
                        elif 'OVER' in upper_val_sum_main: color_class_sum_main = 'stat-normal' 
                        elif 'GTO' in upper_val_sum_main: color_class_sum_main = 'stat-high'   
                            
                        sf.write(f"    <div class='stat-line {color_class_sum_main}'>{label_sum_main}: {v_sum_main}</div>\n")
                sf.write("  </div>\n")
            
            sf.write("""
<script>
function searchResumo() {
  var input = document.getElementById('searchInputResumo');
  var filter = input.value.toUpperCase();
  var divs = document.getElementsByClassName('player-summary');
  for (var i = 0; i < divs.length; i++) {
    var h2 = divs[i].getElementsByTagName('h2')[0];
    if (h2) {
      var txt = h2.textContent || h2.innerText;
      if (txt.toUpperCase().indexOf(filter) > -1) {
        divs[i].style.display = 'block';
      } else {
        divs[i].style.display = 'none';
      }
    }
  }
  if (filter === '') { 
    for (var i = 0; i < divs.length; i++) { divs[i].style.display = 'none'; }
  }
}
document.addEventListener('DOMContentLoaded', function() {
  var divs = document.getElementsByClassName('player-summary');
  for (var i = 0; i < divs.length; i++) { divs[i].style.display = 'none'; }
});
</script>
</body>
</html>""")
        print(f"Resumo salvo em '{output_filename}'.")
    except Exception as e:
        print(f"Erro ao salvar resumo '{output_filename}': {e}")