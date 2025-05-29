import math
from collections import defaultdict
import sqlite3

# Importar as funções de cálculo por street
from stats_calculator_preflop import calculate_preflop_stats_for_player
from stats_calculator_flop import calculate_flop_stats_for_player
from stats_calculator_turn import calculate_turn_stats_for_player 
from stats_calculator_river import calculate_river_stats_for_player

# --- Constantes e Classe PlayerStats como antes ---
# ... (copie POSITION_CATEGORIES, PF_POS_CATS_FOR_STATS, etc.)
# ... (copie a CLASSE PlayerStats completa aqui)
POSITION_CATEGORIES = {
    "UTG": "EP", "UTG+1": "EP", "UTG+2": "EP",
    "MP": "MP", "MP1": "MP", "LJ": "MP", "HJ": "MP",
    "CO": "CO", "BTN": "BTN", "SB": "SB", "BB": "BB"
}
PF_POS_CATS_FOR_STATS = ["EP", "MP", "CO", "BTN", "SB"]
PF_POS_CATS_FOR_CALL_STATS = ["EP", "MP", "CO", "BTN", "SB", "BB"]
def _get_simplified_hand_category_from_description(description_str):
    if not description_str: return "desconhecido"
    desc_lower = description_str.lower()
    topo_keywords = ["straight flush", "four of a kind", "quads", "full house", "flush", "straight", "three of a kind", "two pair"]
    pair_keywords = ["a pair", "one pair"]
    high_card_keywords = ["high card"]
    for kw in topo_keywords:
        if kw in desc_lower: return "topo"
    for kw in pair_keywords:
        if kw in desc_lower: return "bluff_catcher"
    for kw in high_card_keywords:
        if kw in desc_lower: return "air"
    return "desconhecido"
def size_group_factory(): return defaultdict(int)
def line_type_factory(): return defaultdict(size_group_factory)
def dd_int(): return defaultdict(int)
MDF_BE_BY_SIZE_GROUP = { "0-29%": 22.5, "30-45%": 31.0, "46-56%": 35.9, "57-70%": 41.2, "80-100%": 50.0, "101%+": 60.0 }
BLUFF_CLASS_THRESHOLDS = { "0-29%": (18.5, 19.5), "30-45%": (23.68, 24.5), "46-56%": (26.41, 27.5), "57-70%": (29.1, 30.5), "71-100%": (33.0, 34.0), "101%+": (40.0, 41.0) }
FOLD_CLASS_THRESHOLDS = { "0-29%": (22.5, 23.5), "30-45%": (31.0, 32.0), "46-56%": (35.8, 36.9), "57-70%": (41.1, 42.2), "71-100%": (50.0, 51.0), "101%+": (60.0, 61.0) }
def _classify_percentage(size_group, pct, thresholds_dict):
    th = thresholds_dict.get(size_group)
    if not th or pct is None: return None
    under_max, gto_max = th
    if pct <= under_max: return "under"
    elif pct <= gto_max: return "gto"
    return "over"
class PlayerStats:
    # ... (definição completa da classe) ...
    def __init__(self, player_name):
        self.player_name = player_name; self.hands_played = 0
        self.vpip_opportunities = 0; self.vpip_actions = 0
        self.pfr_opportunities = 0; self.pfr_actions = 0
        self.three_bet_pf_opportunities = 0; self.three_bet_pf_actions = 0
        self.fold_to_pf_3bet_opportunities = 0; self.fold_to_pf_3bet_actions = 0
        self.squeeze_pf_opportunities = 0; self.squeeze_pf_actions = 0
        self.four_bet_pf_opportunities = 0; self.four_bet_pf_actions = 0
        self.fold_to_pf_4bet_opportunities = 0; self.fold_to_pf_4bet_actions = 0
        self.fold_bb_vs_btn_steal_opportunities = 0; self.fold_bb_vs_btn_steal_actions = 0
        self.fold_bb_vs_co_steal_opportunities = 0; self.fold_bb_vs_co_steal_actions = 0
        self.fold_bb_vs_sb_steal_opportunities = 0; self.fold_bb_vs_sb_steal_actions = 0
        self.cbet_flop_opportunities = 0; self.cbet_flop_actions = 0
        self.cbet_flop_ip_opportunities = 0; self.cbet_flop_ip_actions = 0
        self.cbet_flop_oop_opportunities = 0; self.cbet_flop_oop_actions = 0
        self.fold_to_flop_cbet_opportunities = 0; self.fold_to_flop_cbet_actions = 0
        self.fold_to_flop_cbet_ip_opportunities = 0; self.fold_to_flop_cbet_ip_actions = 0
        self.fold_to_flop_cbet_oop_opportunities = 0; self.fold_to_flop_cbet_oop_actions = 0
        self.cbet_turn_opportunities = 0; self.cbet_turn_actions = 0
        self.fold_to_turn_cbet_opportunities = 0; self.fold_to_turn_cbet_actions = 0
        self.fold_to_turn_cbet_ip_opportunities = 0; self.fold_to_turn_cbet_ip_actions = 0
        self.fold_to_turn_cbet_oop_opportunities = 0; self.fold_to_turn_cbet_oop_actions = 0
        self.cbet_river_opportunities = 0; self.cbet_river_actions = 0
        self.fold_to_river_cbet_opportunities = 0; self.fold_to_river_cbet_actions = 0
        self.fold_to_river_cbet_ip_opportunities = 0; self.fold_to_river_cbet_ip_actions = 0
        self.fold_to_river_cbet_oop_opportunities = 0; self.fold_to_river_cbet_oop_actions = 0
        self.donk_bet_flop_opportunities = 0; self.donk_bet_flop_actions = 0
        self.fold_to_donk_bet_flop_opportunities = 0; self.fold_to_donk_bet_flop_actions = 0
        self.donk_bet_turn_opportunities = 0; self.donk_bet_turn_actions = 0
        self.fold_to_donk_bet_turn_opportunities = 0; self.fold_to_donk_bet_turn_actions = 0
        self.donk_bet_river_opportunities = 0; self.donk_bet_river_actions = 0
        self.fold_to_donk_bet_river_opportunities = 0; self.fold_to_donk_bet_river_actions = 0
        self.probe_bet_turn_opportunities = 0; self.probe_bet_turn_actions = 0
        self.fold_to_probe_bet_turn_opportunities = 0; self.fold_to_probe_bet_turn_actions = 0
        self.probe_bet_river_opportunities = 0; self.probe_bet_river_actions = 0
        self.fold_to_probe_bet_river_opportunities = 0; self.fold_to_probe_bet_river_actions = 0
        self.bet_vs_missed_cbet_flop_opportunities = 0
        self.bet_vs_missed_cbet_flop_actions = 0
        self.fold_to_bet_vs_missed_cbet_flop_opportunities = 0
        self.fold_to_bet_vs_missed_cbet_flop_actions = 0
        self.bet_vs_missed_cbet_turn_opportunities = 0
        self.bet_vs_missed_cbet_turn_actions = 0
        self.fold_to_bet_vs_missed_cbet_turn_opportunities = 0
        self.fold_to_bet_vs_missed_cbet_turn_actions = 0
        self.bet_vs_missed_cbet_river_opportunities = 0
        self.bet_vs_missed_cbet_river_actions = 0
        self.fold_to_bet_vs_missed_cbet_river_opportunities = 0
        self.fold_to_bet_vs_missed_cbet_river_actions = 0
        self.check_call_flop_opportunities = 0; self.check_call_flop_actions = 0
        self.check_fold_flop_opportunities = 0; self.check_fold_flop_actions = 0
        self.check_raise_flop_opportunities = 0; self.check_raise_flop_actions = 0
        self.fold_to_check_raise_flop_opportunities = 0; self.fold_to_check_raise_flop_actions = 0
        self.check_call_turn_opportunities = 0; self.check_call_turn_actions = 0
        self.check_fold_turn_opportunities = 0; self.check_fold_turn_actions = 0
        self.check_raise_turn_opportunities = 0; self.check_raise_turn_actions = 0
        self.fold_to_check_raise_turn_opportunities = 0; self.fold_to_check_raise_turn_actions = 0
        self.check_call_river_opportunities = 0; self.check_call_river_actions = 0
        self.check_fold_river_opportunities = 0; self.check_fold_river_actions = 0
        self.check_raise_river_opportunities = 0; self.check_raise_river_actions = 0
        self.fold_to_check_raise_river_opportunities = 0; self.fold_to_check_raise_river_actions = 0
        self.pfa_skipped_cbet_then_check_call_flop_opportunities = 0
        self.pfa_skipped_cbet_then_check_call_flop_actions = 0
        self.pfa_skipped_cbet_then_check_fold_flop_opportunities = 0
        self.pfa_skipped_cbet_then_check_fold_flop_actions = 0
        self.pfa_skipped_cbet_then_check_raise_flop_opportunities = 0
        self.pfa_skipped_cbet_then_check_raise_flop_actions = 0
        self.bet_river_opportunities = 0; self.bet_river_actions = 0
        for pos_cat in PF_POS_CATS_FOR_STATS:
            setattr(self, f"open_raise_{pos_cat.lower()}_opportunities", 0)
            setattr(self, f"open_raise_{pos_cat.lower()}_actions", 0)
        for pos_cat in PF_POS_CATS_FOR_CALL_STATS:
            setattr(self, f"call_open_raise_{pos_cat.lower()}_opportunities", 0)
            setattr(self, f"call_open_raise_{pos_cat.lower()}_actions", 0)
        self.fold_to_bet_opportunities_by_size = defaultdict(dd_int)
        self.fold_to_bet_actions_by_size = defaultdict(dd_int)
        self.fold_to_river_bet_by_line_opportunities_by_size = defaultdict(dd_int)
        self.fold_to_river_bet_by_line_actions_by_size = defaultdict(dd_int)
        self.river_bet_called_composition_by_line = defaultdict(line_type_factory)
        self.call_fold_turn_opportunities_by_size = defaultdict(int)
        self.call_fold_turn_actions_by_size = defaultdict(int)
        self.fold_to_flop_cbet_ip_opportunities_by_size = defaultdict(int)
        self.fold_to_flop_cbet_ip_actions_by_size = defaultdict(int)
        self.fold_to_flop_cbet_oop_opportunities_by_size = defaultdict(int)
        self.fold_to_flop_cbet_oop_actions_by_size = defaultdict(int)
        self.fold_to_donk_bet_flop_opportunities_by_size = defaultdict(int)
        self.fold_to_donk_bet_flop_actions_by_size = defaultdict(int)
        self.fold_to_donk_bet_turn_opportunities_by_size = defaultdict(int)
        self.fold_to_donk_bet_turn_actions_by_size = defaultdict(int)
        self.fold_to_donk_bet_river_opportunities_by_size = defaultdict(int)
        self.fold_to_donk_bet_river_actions_by_size = defaultdict(int)
        self.call_call_fold_river_ip_opportunities = 0
        self.call_call_fold_river_ip_actions = 0
        self.call_call_fold_river_oop_opportunities = 0
        self.call_call_fold_river_oop_actions = 0
        self.ccf_triple_barrel_opportunities = 0
        self.ccf_triple_barrel_actions = 0
        self.bbf_vs_donk_river_opportunities = 0
        self.bbf_vs_donk_river_actions = 0

    def get_bet_size_group(self, bet_percentage_pot):
        if bet_percentage_pot is None or math.isnan(bet_percentage_pot) or math.isinf(bet_percentage_pot):
            return "N/A"
        if bet_percentage_pot <= 29.99: return "0-29%"
        if bet_percentage_pot <= 45.99: return "30-45%"
        if bet_percentage_pot <= 56.99: return "46-56%"
        if bet_percentage_pot <= 70.99: return "57-70%"
        if bet_percentage_pot <= 100.99: return "80-100%"
        return "101%+"

    def get_stat_percentage(self, actions, opportunities):
        if opportunities == 0: return 0.0
        return (actions / opportunities) * 100

    def get_raw_stat_value(self, stat_name_key):
        action_attr_map = {
            "VPIP (%)": (self.vpip_actions, self.vpip_opportunities),
            "PFR (%)": (self.pfr_actions, self.pfr_opportunities),
            "3Bet PF (%)": (self.three_bet_pf_actions, self.three_bet_pf_opportunities),
            "Fold to PF 3Bet (%)": (self.fold_to_pf_3bet_actions, self.fold_to_pf_3bet_opportunities),
            "Squeeze PF (%)": (self.squeeze_pf_actions, self.squeeze_pf_opportunities),
            "4Bet PF (%)": (self.four_bet_pf_actions, self.four_bet_pf_opportunities),
            "Fold to PF 4Bet (%)": (self.fold_to_pf_4bet_actions, self.fold_to_pf_4bet_opportunities),
            "Fold BB vs BTN Steal (%)": (self.fold_bb_vs_btn_steal_actions, self.fold_bb_vs_btn_steal_opportunities),
            "Fold BB vs CO Steal (%)": (self.fold_bb_vs_co_steal_actions, self.fold_bb_vs_co_steal_opportunities),
            "Fold BB vs SB Steal (%)": (self.fold_bb_vs_sb_steal_actions, self.fold_bb_vs_sb_steal_opportunities),
            "CBet Flop (%)": (self.cbet_flop_actions, self.cbet_flop_opportunities),
            "CBet Flop IP (%)": (self.cbet_flop_ip_actions, self.cbet_flop_ip_opportunities),
            "CBet Flop OOP (%)": (self.cbet_flop_oop_actions, self.cbet_flop_oop_opportunities),
            "Fold to Flop CBet (%)": (self.fold_to_flop_cbet_actions, self.fold_to_flop_cbet_opportunities),
            "Fold to Flop CBet IP (%)": (self.fold_to_flop_cbet_ip_actions, self.fold_to_flop_cbet_ip_opportunities),
            "Fold to Flop CBet OOP (%)": (self.fold_to_flop_cbet_oop_actions, self.fold_to_flop_cbet_oop_opportunities),
            "Donk Bet Flop (%)": (self.donk_bet_flop_actions, self.donk_bet_flop_opportunities),
            "Fold to Donk Flop (%)": (self.fold_to_donk_bet_flop_actions, self.fold_to_donk_bet_flop_opportunities),
            "Bet vs Missed CBet Flop (%)": (self.bet_vs_missed_cbet_flop_actions, self.bet_vs_missed_cbet_flop_opportunities),
            "Fold to Bet vs Missed CBet Flop (%)": (self.fold_to_bet_vs_missed_cbet_flop_actions, self.fold_to_bet_vs_missed_cbet_flop_opportunities),
            "Check-Call Flop (%)": (self.check_call_flop_actions, self.check_call_flop_opportunities),
            "Check-Fold Flop (%)": (self.check_fold_flop_actions, self.check_fold_flop_opportunities),
            "Check-Raise Flop (%)": (self.check_raise_flop_actions, self.check_raise_flop_opportunities),
            "Fold to XR Flop (%)": (self.fold_to_check_raise_flop_actions, self.fold_to_check_raise_flop_opportunities),
            "PFA SkipCB&XC Flop (%)": (self.pfa_skipped_cbet_then_check_call_flop_actions, self.pfa_skipped_cbet_then_check_call_flop_opportunities),
            "PFA SkipCB&XF Flop (%)": (self.pfa_skipped_cbet_then_check_fold_flop_actions, self.pfa_skipped_cbet_then_check_fold_flop_opportunities),
            "PFA SkipCB&XR Flop (%)": (self.pfa_skipped_cbet_then_check_raise_flop_actions, self.pfa_skipped_cbet_then_check_raise_flop_opportunities),
            "CBet Turn (%)": (self.cbet_turn_actions, self.cbet_turn_opportunities),
            "Fold to Turn CBet (%)": (self.fold_to_turn_cbet_actions, self.fold_to_turn_cbet_opportunities),
            "Fold to Turn CBet IP (%)": (self.fold_to_turn_cbet_ip_actions, self.fold_to_turn_cbet_ip_opportunities),
            "Fold to Turn CBet OOP (%)": (self.fold_to_turn_cbet_oop_actions, self.fold_to_turn_cbet_oop_opportunities),
            "Donk Bet Turn (%)": (self.donk_bet_turn_actions, self.donk_bet_turn_opportunities),
            "Fold to Donk Turn (%)": (self.fold_to_donk_bet_turn_actions, self.fold_to_donk_bet_turn_opportunities),
            "Probe Bet Turn (%)": (self.probe_bet_turn_actions, self.probe_bet_turn_opportunities),
            "Fold to Probe Turn (%)": (self.fold_to_probe_bet_turn_actions, self.fold_to_probe_bet_turn_opportunities),
            "Bet vs Missed CBet Turn (%)": (self.bet_vs_missed_cbet_turn_actions, self.bet_vs_missed_cbet_turn_opportunities),
            "Fold to Bet vs Missed CBet Turn (%)": (self.fold_to_bet_vs_missed_cbet_turn_actions, self.fold_to_bet_vs_missed_cbet_turn_opportunities),
            "Check-Call Turn (%)": (self.check_call_turn_actions, self.check_call_turn_opportunities),
            "Check-Fold Turn (%)": (self.check_fold_turn_actions, self.check_fold_turn_opportunities),
            "Check-Raise Turn (%)": (self.check_raise_turn_actions, self.check_raise_turn_opportunities),
            "Fold to XR Turn (%)": (self.fold_to_check_raise_turn_actions, self.fold_to_check_raise_turn_opportunities),
            "CBet River (%)": (self.cbet_river_actions, self.cbet_river_opportunities),
            "Fold to River CBet (%)": (self.fold_to_river_cbet_actions, self.fold_to_river_cbet_opportunities),
            "Fold to River CBet IP (%)": (self.fold_to_river_cbet_ip_actions, self.fold_to_river_cbet_ip_opportunities),
            "Fold to River CBet OOP (%)": (self.fold_to_river_cbet_oop_actions, self.fold_to_river_cbet_oop_opportunities),
            "Bet River (%)": (self.bet_river_actions, self.bet_river_opportunities),
            "Donk Bet River (%)": (self.donk_bet_river_actions, self.donk_bet_river_opportunities),
            "Fold to Donk River (%)": (self.fold_to_donk_bet_river_actions, self.fold_to_donk_bet_river_opportunities),
            "Probe Bet River (%)": (self.probe_bet_river_actions, self.probe_bet_river_opportunities),
            "Fold to Probe River (%)": (self.fold_to_probe_bet_river_actions, self.fold_to_probe_bet_river_opportunities),
            "Bet vs Missed CBet River (%)": (self.bet_vs_missed_cbet_river_actions, self.bet_vs_missed_cbet_river_opportunities),
            "Fold to Bet vs Missed CBet River (%)": (self.fold_to_bet_vs_missed_cbet_river_actions, self.fold_to_bet_vs_missed_cbet_river_opportunities),
            "Check-Call River (%)": (self.check_call_river_actions, self.check_call_river_opportunities),
            "Check-Fold River (%)": (self.check_fold_river_actions, self.check_fold_river_opportunities),
            "Check-Raise River (%)": (self.check_raise_river_actions, self.check_raise_river_opportunities),
            "Fold to XR River (%)": (self.fold_to_check_raise_river_actions, self.fold_to_check_raise_river_opportunities),
            "CCF vs Triple Barrel (%)": (self.ccf_triple_barrel_actions, self.ccf_triple_barrel_opportunities),
            "BBF vs Donk River (%)": (self.bbf_vs_donk_river_actions, self.bbf_vs_donk_river_opportunities),
        }
        for pos_cat in PF_POS_CATS_FOR_STATS:
            action_attr_map[f"OR {pos_cat} (%)"] = (getattr(self, f"open_raise_{pos_cat.lower()}_actions"), getattr(self, f"open_raise_{pos_cat.lower()}_opportunities"))
        for pos_cat in PF_POS_CATS_FOR_CALL_STATS:
             action_attr_map[f"Call OR {pos_cat} (%)"] = (getattr(self, f"call_open_raise_{pos_cat.lower()}_actions"), getattr(self, f"call_open_raise_{pos_cat.lower()}_opportunities"))
        if stat_name_key in action_attr_map:
            actions, opportunities = action_attr_map[stat_name_key]
            return self.get_stat_percentage(actions, opportunities)
        parts = stat_name_key.split(" ")
        if parts[0] == "FTS" and len(parts) == 4 and parts[3] == "(%)": 
            street, size_group_key = parts[1], parts[2]
            actions = self.fold_to_bet_actions_by_size.get(street, {}).get(size_group_key, 0)
            opportunities = self.fold_to_bet_opportunities_by_size.get(street, {}).get(size_group_key, 0)
            return self.get_stat_percentage(actions, opportunities)
        if parts[0] == "Fold" and parts[1] == "CBet" and parts[2] == "Flop" and parts[3] in ["IP", "OOP"] and len(parts) == 6 and parts[5] == "(%)": 
            ip = parts[3] == "IP"
            size_group_key = parts[4]
            if ip:
                actions = self.fold_to_flop_cbet_ip_actions_by_size.get(size_group_key, 0)
                opportunities = self.fold_to_flop_cbet_ip_opportunities_by_size.get(size_group_key, 0)
            else: 
                actions = self.fold_to_flop_cbet_oop_actions_by_size.get(size_group_key, 0)
                opportunities = self.fold_to_flop_cbet_oop_opportunities_by_size.get(size_group_key, 0)
            return self.get_stat_percentage(actions, opportunities)
        if parts[0] == "Fold" and parts[1] == "Donk" and parts[2] in ["Flop", "Turn", "River"] and len(parts) == 5 and parts[4] == "(%)": 
            street = parts[2]
            size_group_key = parts[3]
            actions = getattr(self, f"fold_to_donk_bet_{street.lower()}_actions_by_size").get(size_group_key, 0)
            opportunities = getattr(self, f"fold_to_donk_bet_{street.lower()}_opportunities_by_size").get(size_group_key, 0)
            return self.get_stat_percentage(actions, opportunities)
        if parts[0] == "FTS" and len(parts) == 5 and parts[1] == "River" and parts[4] == "(%)": 
            line_type, size_group_key = parts[2], parts[3]
            actions = self.fold_to_river_bet_by_line_actions_by_size.get(line_type, {}).get(size_group_key, 0)
            opportunities = self.fold_to_river_bet_by_line_opportunities_by_size.get(line_type, {}).get(size_group_key, 0)
            return self.get_stat_percentage(actions, opportunities)
        if stat_name_key.startswith("River ") and stat_name_key.endswith(" (%)") and len(parts) == 5: 
            try:
                line_type, size_group, hand_cat_display = parts[1], parts[2], parts[3]
                hc_map_inv = {"Topo": "topo", "BluffCatcher": "bluff_catcher", "Air": "air"}
                hand_cat_internal = hc_map_inv.get(hand_cat_display)
                if hand_cat_internal:
                    line_data = self.river_bet_called_composition_by_line.get(line_type, {})
                    size_data = line_data.get(size_group, {})
                    actions = size_data.get(hand_cat_internal, 0)
                    opportunities = size_data.get('total_showdowns', 0)
                    return self.get_stat_percentage(actions, opportunities)
            except Exception: pass 
        if parts[0] == "CF" and parts[1] == "Turn" and len(parts) == 4 and parts[3] == "(%)": 
            size_group_key = parts[2]
            actions = self.call_fold_turn_actions_by_size.get(size_group_key, 0)
            opportunities = self.call_fold_turn_opportunities_by_size.get(size_group_key, 0)
            return self.get_stat_percentage(actions, opportunities)
        return 0.0
    # --- Propriedades de Display ---
    # (Cole todas as suas propriedades de display aqui)
    @property
    def vpip_percentage_display(self): return f"{self.get_stat_percentage(self.vpip_actions, self.vpip_opportunities):.1f}% ({self.vpip_actions}/{self.vpip_opportunities})"
    @property
    def pfr_percentage_display(self): return f"{self.get_stat_percentage(self.pfr_actions, self.pfr_opportunities):.1f}% ({self.pfr_actions}/{self.pfr_opportunities})"
    @property
    def three_bet_pf_percentage_display(self): return f"{self.get_stat_percentage(self.three_bet_pf_actions, self.three_bet_pf_opportunities):.1f}% ({self.three_bet_pf_actions}/{self.three_bet_pf_opportunities})"
    @property
    def fold_to_pf_3bet_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_pf_3bet_actions, self.fold_to_pf_3bet_opportunities):.1f}% ({self.fold_to_pf_3bet_actions}/{self.fold_to_pf_3bet_opportunities})"
    @property
    def squeeze_pf_percentage_display(self): return f"{self.get_stat_percentage(self.squeeze_pf_actions, self.squeeze_pf_opportunities):.1f}% ({self.squeeze_pf_actions}/{self.squeeze_pf_opportunities})"
    @property
    def four_bet_pf_percentage_display(self): return f"{self.get_stat_percentage(self.four_bet_pf_actions, self.four_bet_pf_opportunities):.1f}% ({self.four_bet_pf_actions}/{self.four_bet_pf_opportunities})"
    @property
    def fold_to_pf_4bet_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_pf_4bet_actions, self.fold_to_pf_4bet_opportunities):.1f}% ({self.fold_to_pf_4bet_actions}/{self.fold_to_pf_4bet_opportunities})"
    @property
    def fold_bb_vs_btn_steal_percentage_display(self): return f"{self.get_stat_percentage(self.fold_bb_vs_btn_steal_actions, self.fold_bb_vs_btn_steal_opportunities):.1f}% ({self.fold_bb_vs_btn_steal_actions}/{self.fold_bb_vs_btn_steal_opportunities})"
    @property
    def fold_bb_vs_co_steal_percentage_display(self): return f"{self.get_stat_percentage(self.fold_bb_vs_co_steal_actions, self.fold_bb_vs_co_steal_opportunities):.1f}% ({self.fold_bb_vs_co_steal_actions}/{self.fold_bb_vs_co_steal_opportunities})"
    @property
    def fold_bb_vs_sb_steal_percentage_display(self): return f"{self.get_stat_percentage(self.fold_bb_vs_sb_steal_actions, self.fold_bb_vs_sb_steal_opportunities):.1f}% ({self.fold_bb_vs_sb_steal_actions}/{self.fold_bb_vs_sb_steal_opportunities})"
    @property
    def cbet_flop_percentage_display(self): return f"{self.get_stat_percentage(self.cbet_flop_actions, self.cbet_flop_opportunities):.1f}% ({self.cbet_flop_actions}/{self.cbet_flop_opportunities})"
    @property
    def cbet_flop_ip_percentage_display(self): return f"{self.get_stat_percentage(self.cbet_flop_ip_actions, self.cbet_flop_ip_opportunities):.1f}% ({self.cbet_flop_ip_actions}/{self.cbet_flop_ip_opportunities})"
    @property
    def cbet_flop_oop_percentage_display(self): return f"{self.get_stat_percentage(self.cbet_flop_oop_actions, self.cbet_flop_oop_opportunities):.1f}% ({self.cbet_flop_oop_actions}/{self.cbet_flop_oop_opportunities})"
    @property
    def fold_to_flop_cbet_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_flop_cbet_actions, self.fold_to_flop_cbet_opportunities):.1f}% ({self.fold_to_flop_cbet_actions}/{self.fold_to_flop_cbet_opportunities})"
    @property
    def fold_to_flop_cbet_ip_percentage_display(self):
        return f"{self.get_stat_percentage(self.fold_to_flop_cbet_ip_actions, self.fold_to_flop_cbet_ip_opportunities):.1f}% ({self.fold_to_flop_cbet_ip_actions}/{self.fold_to_flop_cbet_ip_opportunities})"
    @property
    def fold_to_flop_cbet_oop_percentage_display(self):
        return f"{self.get_stat_percentage(self.fold_to_flop_cbet_oop_actions, self.fold_to_flop_cbet_oop_opportunities):.1f}% ({self.fold_to_flop_cbet_oop_actions}/{self.fold_to_flop_cbet_oop_opportunities})"
    def get_fold_to_flop_cbet_by_size_display(self, ip, size_group):
        if ip:
            actions = self.fold_to_flop_cbet_ip_actions_by_size.get(size_group, 0)
            opps = self.fold_to_flop_cbet_ip_opportunities_by_size.get(size_group, 0)
        else:
            actions = self.fold_to_flop_cbet_oop_actions_by_size.get(size_group, 0)
            opps = self.fold_to_flop_cbet_oop_opportunities_by_size.get(size_group, 0)
        pct = self.get_stat_percentage(actions, opps)
        label = _classify_percentage(size_group, pct, FOLD_CLASS_THRESHOLDS)
        label_txt = f" {label.capitalize()}" if label else ""
        return f"{pct:.1f}% ({actions}/{opps}){label_txt}"
    def get_fold_to_donk_bet_by_size_display(self, street, size_group):
        act_attr = f"fold_to_donk_bet_{street.lower()}_actions_by_size"
        opp_attr = f"fold_to_donk_bet_{street.lower()}_opportunities_by_size"
        actions = getattr(self, act_attr).get(size_group, 0)
        opps = getattr(self, opp_attr).get(size_group, 0)
        pct = self.get_stat_percentage(actions, opps)
        label = _classify_percentage(size_group, pct, FOLD_CLASS_THRESHOLDS)
        label_txt = f" {label.capitalize()}" if label else ""
        return f"{pct:.1f}% ({actions}/{opps}){label_txt}"
    @property
    def call_call_fold_river_ip_percentage_display(self):
        return f"{self.get_stat_percentage(self.call_call_fold_river_ip_actions, self.call_call_fold_river_ip_opportunities):.1f}% ({self.call_call_fold_river_ip_actions}/{self.call_call_fold_river_ip_opportunities})"
    @property
    def call_call_fold_river_oop_percentage_display(self):
        return f"{self.get_stat_percentage(self.call_call_fold_river_oop_actions, self.call_call_fold_river_oop_opportunities):.1f}% ({self.call_call_fold_river_oop_actions}/{self.call_call_fold_river_oop_opportunities})"
    @property
    def ccf_triple_barrel_percentage_display(self):
        return f"{self.get_stat_percentage(self.ccf_triple_barrel_actions, self.ccf_triple_barrel_opportunities):.1f}% ({self.ccf_triple_barrel_actions}/{self.ccf_triple_barrel_opportunities})"
    @property
    def bbf_vs_donk_river_percentage_display(self):
        return f"{self.get_stat_percentage(self.bbf_vs_donk_river_actions, self.bbf_vs_donk_river_opportunities):.1f}% ({self.bbf_vs_donk_river_actions}/{self.bbf_vs_donk_river_opportunities})"
    @property
    def donk_bet_flop_percentage_display(self): return f"{self.get_stat_percentage(self.donk_bet_flop_actions, self.donk_bet_flop_opportunities):.1f}% ({self.donk_bet_flop_actions}/{self.donk_bet_flop_opportunities})"
    @property
    def fold_to_donk_bet_flop_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_donk_bet_flop_actions, self.fold_to_donk_bet_flop_opportunities):.1f}% ({self.fold_to_donk_bet_flop_actions}/{self.fold_to_donk_bet_flop_opportunities})"
    @property
    def bet_vs_missed_cbet_flop_percentage_display(self): return f"{self.get_stat_percentage(self.bet_vs_missed_cbet_flop_actions, self.bet_vs_missed_cbet_flop_opportunities):.1f}% ({self.bet_vs_missed_cbet_flop_actions}/{self.bet_vs_missed_cbet_flop_opportunities})"
    @property
    def fold_to_bet_vs_missed_cbet_flop_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_bet_vs_missed_cbet_flop_actions, self.fold_to_bet_vs_missed_cbet_flop_opportunities):.1f}% ({self.fold_to_bet_vs_missed_cbet_flop_actions}/{self.fold_to_bet_vs_missed_cbet_flop_opportunities})"
    @property
    def check_call_flop_percentage_display(self): return f"{self.get_stat_percentage(self.check_call_flop_actions, self.check_call_flop_opportunities):.1f}% ({self.check_call_flop_actions}/{self.check_call_flop_opportunities})"
    @property
    def check_fold_flop_percentage_display(self): return f"{self.get_stat_percentage(self.check_fold_flop_actions, self.check_fold_flop_opportunities):.1f}% ({self.check_fold_flop_actions}/{self.check_fold_flop_opportunities})"
    @property
    def check_raise_flop_percentage_display(self): return f"{self.get_stat_percentage(self.check_raise_flop_actions, self.check_raise_flop_opportunities):.1f}% ({self.check_raise_flop_actions}/{self.check_raise_flop_opportunities})"
    @property
    def fold_to_check_raise_flop_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_check_raise_flop_actions, self.fold_to_check_raise_flop_opportunities):.1f}% ({self.fold_to_check_raise_flop_actions}/{self.fold_to_check_raise_flop_opportunities})"
    @property
    def pfa_skipped_cbet_then_xc_flop_percentage_display(self): return f"{self.get_stat_percentage(self.pfa_skipped_cbet_then_check_call_flop_actions, self.pfa_skipped_cbet_then_check_call_flop_opportunities):.1f}% ({self.pfa_skipped_cbet_then_check_call_flop_actions}/{self.pfa_skipped_cbet_then_check_call_flop_opportunities})"
    @property
    def pfa_skipped_cbet_then_xf_flop_percentage_display(self): return f"{self.get_stat_percentage(self.pfa_skipped_cbet_then_check_fold_flop_actions, self.pfa_skipped_cbet_then_check_fold_flop_opportunities):.1f}% ({self.pfa_skipped_cbet_then_check_fold_flop_actions}/{self.pfa_skipped_cbet_then_check_fold_flop_opportunities})"
    @property
    def pfa_skipped_cbet_then_xr_flop_percentage_display(self): return f"{self.get_stat_percentage(self.pfa_skipped_cbet_then_check_raise_flop_actions, self.pfa_skipped_cbet_then_check_raise_flop_opportunities):.1f}% ({self.pfa_skipped_cbet_then_check_raise_flop_actions}/{self.pfa_skipped_cbet_then_check_raise_flop_opportunities})"
    @property
    def cbet_turn_percentage_display(self): return f"{self.get_stat_percentage(self.cbet_turn_actions, self.cbet_turn_opportunities):.1f}% ({self.cbet_turn_actions}/{self.cbet_turn_opportunities})"
    @property
    def fold_to_turn_cbet_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_turn_cbet_actions, self.fold_to_turn_cbet_opportunities):.1f}% ({self.fold_to_turn_cbet_actions}/{self.fold_to_turn_cbet_opportunities})"
    @property
    def fold_to_turn_cbet_ip_percentage_display(self):
        return f"{self.get_stat_percentage(self.fold_to_turn_cbet_ip_actions, self.fold_to_turn_cbet_ip_opportunities):.1f}% ({self.fold_to_turn_cbet_ip_actions}/{self.fold_to_turn_cbet_ip_opportunities})"
    @property
    def fold_to_turn_cbet_oop_percentage_display(self):
        return f"{self.get_stat_percentage(self.fold_to_turn_cbet_oop_actions, self.fold_to_turn_cbet_oop_opportunities):.1f}% ({self.fold_to_turn_cbet_oop_actions}/{self.fold_to_turn_cbet_oop_opportunities})"
    @property
    def donk_bet_turn_percentage_display(self): return f"{self.get_stat_percentage(self.donk_bet_turn_actions, self.donk_bet_turn_opportunities):.1f}% ({self.donk_bet_turn_actions}/{self.donk_bet_turn_opportunities})"
    @property
    def fold_to_donk_bet_turn_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_donk_bet_turn_actions, self.fold_to_donk_bet_turn_opportunities):.1f}% ({self.fold_to_donk_bet_turn_actions}/{self.fold_to_donk_bet_turn_opportunities})"
    @property
    def probe_bet_turn_percentage_display(self): return f"{self.get_stat_percentage(self.probe_bet_turn_actions, self.probe_bet_turn_opportunities):.1f}% ({self.probe_bet_turn_actions}/{self.probe_bet_turn_opportunities})"
    @property
    def fold_to_probe_bet_turn_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_probe_bet_turn_actions, self.fold_to_probe_bet_turn_opportunities):.1f}% ({self.fold_to_probe_bet_turn_actions}/{self.fold_to_probe_bet_turn_opportunities})"
    @property
    def bet_vs_missed_cbet_turn_percentage_display(self): return f"{self.get_stat_percentage(self.bet_vs_missed_cbet_turn_actions, self.bet_vs_missed_cbet_turn_opportunities):.1f}% ({self.bet_vs_missed_cbet_turn_actions}/{self.bet_vs_missed_cbet_turn_opportunities})"
    @property
    def fold_to_bet_vs_missed_cbet_turn_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_bet_vs_missed_cbet_turn_actions, self.fold_to_bet_vs_missed_cbet_turn_opportunities):.1f}% ({self.fold_to_bet_vs_missed_cbet_turn_actions}/{self.fold_to_bet_vs_missed_cbet_turn_opportunities})"
    @property
    def check_call_turn_percentage_display(self): return f"{self.get_stat_percentage(self.check_call_turn_actions, self.check_call_turn_opportunities):.1f}% ({self.check_call_turn_actions}/{self.check_call_turn_opportunities})"
    @property
    def check_fold_turn_percentage_display(self): return f"{self.get_stat_percentage(self.check_fold_turn_actions, self.check_fold_turn_opportunities):.1f}% ({self.check_fold_turn_actions}/{self.check_fold_turn_opportunities})"
    @property
    def check_raise_turn_percentage_display(self): return f"{self.get_stat_percentage(self.check_raise_turn_actions, self.check_raise_turn_opportunities):.1f}% ({self.check_raise_turn_actions}/{self.check_raise_turn_opportunities})"
    @property
    def fold_to_check_raise_turn_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_check_raise_turn_actions, self.fold_to_check_raise_turn_opportunities):.1f}% ({self.fold_to_check_raise_turn_actions}/{self.fold_to_check_raise_turn_opportunities})"
    @property
    def cbet_river_percentage_display(self): return f"{self.get_stat_percentage(self.cbet_river_actions, self.cbet_river_opportunities):.1f}% ({self.cbet_river_actions}/{self.cbet_river_opportunities})"
    @property
    def fold_to_river_cbet_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_river_cbet_actions, self.fold_to_river_cbet_opportunities):.1f}% ({self.fold_to_river_cbet_actions}/{self.fold_to_river_cbet_opportunities})"
    @property
    def fold_to_river_cbet_ip_percentage_display(self):
        return f"{self.get_stat_percentage(self.fold_to_river_cbet_ip_actions, self.fold_to_river_cbet_ip_opportunities):.1f}% ({self.fold_to_river_cbet_ip_actions}/{self.fold_to_river_cbet_ip_opportunities})"
    @property
    def fold_to_river_cbet_oop_percentage_display(self):
        return f"{self.get_stat_percentage(self.fold_to_river_cbet_oop_actions, self.fold_to_river_cbet_oop_opportunities):.1f}% ({self.fold_to_river_cbet_oop_actions}/{self.fold_to_river_cbet_oop_opportunities})"
    @property
    def bet_river_percentage_display(self): return f"{self.get_stat_percentage(self.bet_river_actions, self.bet_river_opportunities):.1f}% ({self.bet_river_actions}/{self.bet_river_opportunities})"
    @property
    def donk_bet_river_percentage_display(self): return f"{self.get_stat_percentage(self.donk_bet_river_actions, self.donk_bet_river_opportunities):.1f}% ({self.donk_bet_river_actions}/{self.donk_bet_river_opportunities})"
    @property
    def fold_to_donk_bet_river_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_donk_bet_river_actions, self.fold_to_donk_bet_river_opportunities):.1f}% ({self.fold_to_donk_bet_river_actions}/{self.fold_to_donk_bet_river_opportunities})"
    @property
    def probe_bet_river_percentage_display(self): return f"{self.get_stat_percentage(self.probe_bet_river_actions, self.probe_bet_river_opportunities):.1f}% ({self.probe_bet_river_actions}/{self.probe_bet_river_opportunities})"
    @property
    def fold_to_probe_bet_river_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_probe_bet_river_actions, self.fold_to_probe_bet_river_opportunities):.1f}% ({self.fold_to_probe_bet_river_actions}/{self.fold_to_probe_bet_river_opportunities})"
    @property
    def bet_vs_missed_cbet_river_percentage_display(self): return f"{self.get_stat_percentage(self.bet_vs_missed_cbet_river_actions, self.bet_vs_missed_cbet_river_opportunities):.1f}% ({self.bet_vs_missed_cbet_river_actions}/{self.bet_vs_missed_cbet_river_opportunities})"
    @property
    def fold_to_bet_vs_missed_cbet_river_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_bet_vs_missed_cbet_river_actions, self.fold_to_bet_vs_missed_cbet_river_opportunities):.1f}% ({self.fold_to_bet_vs_missed_cbet_river_actions}/{self.fold_to_bet_vs_missed_cbet_river_opportunities})"
    @property
    def check_call_river_percentage_display(self): return f"{self.get_stat_percentage(self.check_call_river_actions, self.check_call_river_opportunities):.1f}% ({self.check_call_river_actions}/{self.check_call_river_opportunities})"
    @property
    def check_fold_river_percentage_display(self): return f"{self.get_stat_percentage(self.check_fold_river_actions, self.check_fold_river_opportunities):.1f}% ({self.check_fold_river_actions}/{self.check_fold_river_opportunities})"
    @property
    def check_raise_river_percentage_display(self): return f"{self.get_stat_percentage(self.check_raise_river_actions, self.check_raise_river_opportunities):.1f}% ({self.check_raise_river_actions}/{self.check_raise_river_opportunities})"
    @property
    def fold_to_check_raise_river_percentage_display(self): return f"{self.get_stat_percentage(self.fold_to_check_raise_river_actions, self.fold_to_check_raise_river_opportunities):.1f}% ({self.fold_to_check_raise_river_actions}/{self.fold_to_check_raise_river_opportunities})"
    def _get_positional_stat_display(self, base_name, pos_cat):
        opp_attr = f"{base_name}_{pos_cat.lower()}_opportunities"
        act_attr = f"{base_name}_{pos_cat.lower()}_actions"
        opps = getattr(self, opp_attr, 0)
        acts = getattr(self, act_attr, 0)
        return f"{self.get_stat_percentage(acts, opps):.1f}% ({acts}/{opps})"
    @property
    def open_raise_ep_display(self): return self._get_positional_stat_display("open_raise", "EP")
    @property
    def open_raise_mp_display(self): return self._get_positional_stat_display("open_raise", "MP")
    @property
    def open_raise_co_display(self): return self._get_positional_stat_display("open_raise", "CO")
    @property
    def open_raise_btn_display(self): return self._get_positional_stat_display("open_raise", "BTN")
    @property
    def open_raise_sb_display(self): return self._get_positional_stat_display("open_raise", "SB")
    @property
    def call_open_raise_ep_display(self): return self._get_positional_stat_display("call_open_raise", "EP")
    @property
    def call_open_raise_mp_display(self): return self._get_positional_stat_display("call_open_raise", "MP")
    @property
    def call_open_raise_co_display(self): return self._get_positional_stat_display("call_open_raise", "CO")
    @property
    def call_open_raise_btn_display(self): return self._get_positional_stat_display("call_open_raise", "BTN")
    @property
    def call_open_raise_sb_display(self): return self._get_positional_stat_display("call_open_raise", "SB")
    @property
    def call_open_raise_bb_display(self): return self._get_positional_stat_display("call_open_raise", "BB")
    def get_fold_to_bet_by_size_display(self, street, size_group):
        actions = self.fold_to_bet_actions_by_size.get(street, {}).get(size_group, 0)
        opportunities = self.fold_to_bet_opportunities_by_size.get(street, {}).get(size_group, 0)
        pct = self.get_stat_percentage(actions, opportunities)
        label = _classify_percentage(size_group, pct, FOLD_CLASS_THRESHOLDS)
        label_txt = f" {label.capitalize()}" if label else ""
        return f"{pct:.1f}% ({actions}/{opportunities}){label_txt}"
    def get_fold_to_river_bet_by_line_display(self, line_type, size_group):
        actions = self.fold_to_river_bet_by_line_actions_by_size.get(line_type, {}).get(size_group, 0)
        opportunities = self.fold_to_river_bet_by_line_opportunities_by_size.get(line_type, {}).get(size_group, 0)
        pct = self.get_stat_percentage(actions, opportunities)
        label = _classify_percentage(size_group, pct, FOLD_CLASS_THRESHOLDS)
        label_txt = f" {label.capitalize()}" if label else ""
        return f"{pct:.1f}% ({actions}/{opportunities}){label_txt}"
    def get_call_fold_turn_display(self, size_group):
        actions = self.call_fold_turn_actions_by_size.get(size_group, 0)
        opportunities = self.call_fold_turn_opportunities_by_size.get(size_group, 0)
        pct = self.get_stat_percentage(actions, opportunities)
        label = _classify_percentage(size_group, pct, FOLD_CLASS_THRESHOLDS)
        label_txt = f" {label.capitalize()}" if label else ""
        return f"{pct:.1f}% ({actions}/{opportunities}){label_txt}"
    def get_river_bet_composition_by_line_display(self, line_type, size_group, hand_category_key):
        line_data = self.river_bet_called_composition_by_line.get(line_type, defaultdict(dd_int))
        size_data = line_data.get(size_group, defaultdict(int))
        count = size_data.get(hand_category_key, 0)
        total_showdowns = size_data.get('total_showdowns', 0)
        percentage = self.get_stat_percentage(count, total_showdowns)
        result = f"{percentage:.1f}% ({count}/{total_showdowns})"
        if hand_category_key == 'air': 
            label = _classify_percentage(size_group, percentage, BLUFF_CLASS_THRESHOLDS)
            if label:
                result += f" {label.capitalize()}"
        return result
    def _get_river_bluff_value_counts(self, line_type, size_group):
        line_data = self.river_bet_called_composition_by_line.get(line_type, {})
        size_data = line_data.get(size_group, {})
        total = size_data.get('total_showdowns', 0)
        bluff = size_data.get('air', 0) 
        return bluff, total
    def get_river_bluff_percentage_display(self, line_type, size_group):
        bluff, total = self._get_river_bluff_value_counts(line_type, size_group)
        return f"{self.get_stat_percentage(bluff, total):.1f}% ({bluff}/{total})"
    def get_river_value_percentage_display(self, line_type, size_group):
        bluff, total = self._get_river_bluff_value_counts(line_type, size_group)
        value = total - bluff 
        return f"{self.get_stat_percentage(value, total):.1f}% ({value}/{total})"
    def get_river_bluff_over_under_display(self, line_type, size_group):
        bluff, total = self._get_river_bluff_value_counts(line_type, size_group)
        if total == 0: return "N/A"
        pct = self.get_stat_percentage(bluff, total)
        label_key = _classify_percentage(size_group, pct, BLUFF_CLASS_THRESHOLDS)
        if not label_key: return f"{pct:.1f}%"
        label_map = {"under": "Under Blefa", "gto": "GTO Blefe", "over": "Over Blefa"}
        return f"{label_map[label_key]} ({pct:.1f}%)"
    def to_dict_display(self):
        d = {
            "Player": self.player_name, "Hands Played": str(self.hands_played),
            "VPIP (%)": self.vpip_percentage_display,
            "PFR (%)": self.pfr_percentage_display,
            "3Bet PF (%)": self.three_bet_pf_percentage_display,
            "Fold to PF 3Bet (%)": self.fold_to_pf_3bet_percentage_display,
            "Squeeze PF (%)": self.squeeze_pf_percentage_display,
            "4Bet PF (%)": self.four_bet_pf_percentage_display,
            "Fold to PF 4Bet (%)": self.fold_to_pf_4bet_percentage_display,
            "Fold BB vs BTN Steal (%)": self.fold_bb_vs_btn_steal_percentage_display,
            "Fold BB vs CO Steal (%)": self.fold_bb_vs_co_steal_percentage_display,
            "Fold BB vs SB Steal (%)": self.fold_bb_vs_sb_steal_percentage_display,
            "CBet Flop (%)": self.cbet_flop_percentage_display,
            "CBet Flop IP (%)": self.cbet_flop_ip_percentage_display,
            "CBet Flop OOP (%)": self.cbet_flop_oop_percentage_display,
            "Fold to Flop CBet (%)": self.fold_to_flop_cbet_percentage_display,
            "Fold to Flop CBet IP (%)": self.fold_to_flop_cbet_ip_percentage_display,
            "Fold to Flop CBet OOP (%)": self.fold_to_flop_cbet_oop_percentage_display,
            "Donk Bet Flop (%)": self.donk_bet_flop_percentage_display,
            "Fold to Donk Flop (%)": self.fold_to_donk_bet_flop_percentage_display,
            "Bet vs Missed CBet Flop (%)": self.bet_vs_missed_cbet_flop_percentage_display,
            "Fold to Bet vs Missed CBet Flop (%)": self.fold_to_bet_vs_missed_cbet_flop_percentage_display,
            "Check-Call Flop (%)": self.check_call_flop_percentage_display,
            "Check-Fold Flop (%)": self.check_fold_flop_percentage_display,
            "Check-Raise Flop (%)": self.check_raise_flop_percentage_display,
            "Fold to XR Flop (%)" : self.fold_to_check_raise_flop_percentage_display,
            "PFA SkipCB&XC Flop (%)": self.pfa_skipped_cbet_then_xc_flop_percentage_display,
            "PFA SkipCB&XF Flop (%)": self.pfa_skipped_cbet_then_xf_flop_percentage_display,
            "PFA SkipCB&XR Flop (%)": self.pfa_skipped_cbet_then_xr_flop_percentage_display,
            "CBet Turn (%)": self.cbet_turn_percentage_display,
            "Fold to Turn CBet (%)": self.fold_to_turn_cbet_percentage_display,
            "Fold to Turn CBet IP (%)": self.fold_to_turn_cbet_ip_percentage_display,
            "Fold to Turn CBet OOP (%)": self.fold_to_turn_cbet_oop_percentage_display,
            "Donk Bet Turn (%)": self.donk_bet_turn_percentage_display,
            "Fold to Donk Turn (%)": self.fold_to_donk_bet_turn_percentage_display,
            "Probe Bet Turn (%)": self.probe_bet_turn_percentage_display,
            "Fold to Probe Turn (%)": self.fold_to_probe_bet_turn_percentage_display,
            "Bet vs Missed CBet Turn (%)": self.bet_vs_missed_cbet_turn_percentage_display,
            "Fold to Bet vs Missed CBet Turn (%)": self.fold_to_bet_vs_missed_cbet_turn_percentage_display,
            "Check-Call Turn (%)": self.check_call_turn_percentage_display,
            "Check-Fold Turn (%)": self.check_fold_turn_percentage_display,
            "Check-Raise Turn (%)": self.check_raise_turn_percentage_display,
            "Fold to XR Turn (%)": self.fold_to_check_raise_turn_percentage_display,
            "CBet River (%)": self.cbet_river_percentage_display,
            "Fold to River CBet (%)": self.fold_to_river_cbet_percentage_display,
            "Fold to River CBet IP (%)": self.fold_to_river_cbet_ip_percentage_display,
            "Fold to River CBet OOP (%)": self.fold_to_river_cbet_oop_percentage_display,
            "Bet River (%)": self.bet_river_percentage_display,
            "Donk Bet River (%)": self.donk_bet_river_percentage_display,
            "Fold to Donk River (%)": self.fold_to_donk_bet_river_percentage_display,
            "Probe Bet River (%)": self.probe_bet_river_percentage_display,
            "Fold to Probe River (%)": self.fold_to_probe_bet_river_percentage_display,
            "Bet vs Missed CBet River (%)": self.bet_vs_missed_cbet_river_percentage_display,
            "Fold to Bet vs Missed CBet River (%)": self.fold_to_bet_vs_missed_cbet_river_percentage_display,
            "Check-Call River (%)": self.check_call_river_percentage_display,
            "Check-Fold River (%)": self.check_fold_river_percentage_display,
            "Check-Raise River (%)": self.check_raise_river_percentage_display,
            "Fold to XR River (%)": self.fold_to_check_raise_river_percentage_display,
            "CCF River IP (%)": self.call_call_fold_river_ip_percentage_display,
            "CCF River OOP (%)": self.call_call_fold_river_oop_percentage_display,
            "CCF vs Triple Barrel (%)": self.ccf_triple_barrel_percentage_display,
            "BBF vs Donk River (%)": self.bbf_vs_donk_river_percentage_display,
        }
        for pos_cat in PF_POS_CATS_FOR_STATS:
            d[f"OR {pos_cat} (%)"] = self._get_positional_stat_display("open_raise", pos_cat)
        for pos_cat in PF_POS_CATS_FOR_CALL_STATS:
            d[f"Call OR {pos_cat} (%)"] = self._get_positional_stat_display("call_open_raise", pos_cat)
        size_groups_for_dict = ["0-29%", "30-45%", "46-56%", "57-70%", "80-100%", "101%+", "N/A"]
        line_types_for_dict = ["BBB", "BXB", "XBB", "XXB"] 
        for street in ["Flop", "Turn", "River"]:
            for sg in size_groups_for_dict:
                if sg == "N/A" : continue
                key = f"FTS {street} {sg} (%)" 
                d[key] = self.get_fold_to_bet_by_size_display(street, sg)
        for sg in size_groups_for_dict:
            if sg == "N/A": continue
            d[f"Fold CBet Flop IP {sg} (%)"] = self.get_fold_to_flop_cbet_by_size_display(True, sg)
            d[f"Fold CBet Flop OOP {sg} (%)"] = self.get_fold_to_flop_cbet_by_size_display(False, sg)
            d[f"Fold Donk Flop {sg} (%)"] = self.get_fold_to_donk_bet_by_size_display('Flop', sg)
            d[f"Fold Donk Turn {sg} (%)"] = self.get_fold_to_donk_bet_by_size_display('Turn', sg)
            d[f"Fold Donk River {sg} (%)"] = self.get_fold_to_donk_bet_by_size_display('River', sg)
        for lt in line_types_for_dict:
            for sg in size_groups_for_dict:
                if sg == "N/A": continue
                key = f"FTS River {lt} {sg} (%)"
                d[key] = self.get_fold_to_river_bet_by_line_display(lt, sg)
        for sg in size_groups_for_dict:
            if sg == "N/A": continue
            key = f"CF Turn {sg} (%)" 
            d[key] = self.get_call_fold_turn_display(sg)
        hand_categories_display_map_for_dict = {"topo": "Topo", "bluff_catcher": "BluffCatcher", "air": "Air"}
        for lt in line_types_for_dict:
            for sg in size_groups_for_dict:
                if sg == "N/A": continue
                line_data = self.river_bet_called_composition_by_line.get(lt)
                if line_data:
                    size_data = line_data.get(sg)
                    if size_data and size_data.get('total_showdowns', 0) > 0:
                        for hc_key, hc_display in hand_categories_display_map_for_dict.items():
                            d[f"River {lt} {sg} {hc_display} (%)"] = self.get_river_bet_composition_by_line_display(lt, sg, hc_key)
                        d[f"River {lt} {sg} Bluff (%)"] = self.get_river_bluff_percentage_display(lt, sg)
                        d[f"River {lt} {sg} Value (%)"] = self.get_river_value_percentage_display(lt, sg)
                        d[f"River {lt} {sg} Bluff vs MDF"] = self.get_river_bluff_over_under_display(lt, sg)
        return d

def player_stats_factory(): # Adicionado para compatibilidade com defaultdict
    return PlayerStats(None)


def calculate_stats_for_single_player(conn: sqlite3.Connection, player_id: int, player_name: str) -> PlayerStats:
    """
    Calcula TODAS as estatísticas para UM jogador específico a partir do banco de dados.
    Chama funções auxiliares para cada street.
    """
    ps = PlayerStats(player_name) # Cria o objeto de estatísticas
    cursor = conn.cursor()

    # --- Hands Played (calculado uma vez) ---
    cursor.execute("SELECT COUNT(DISTINCT hand_db_id) FROM hand_players WHERE player_id = ?", (player_id,))
    count_row = cursor.fetchone()
    ps.hands_played = count_row[0] if count_row and count_row[0] is not None else 0

    if ps.hands_played == 0:
        print(f"Jogador {player_name} (ID: {player_id}) não tem mãos jogadas. Pulando cálculo de stats.")
        return ps # Retorna stats zeradas

    print(f"  Calculando stats Pré-Flop para {player_name}...")
    calculate_preflop_stats_for_player(ps, cursor, player_id)
    
    print(f"  Calculando stats de Flop para {player_name}...")
    calculate_flop_stats_for_player(ps, cursor, player_id)
    
    # print(f"  Calculando stats de Turn para {player_name}...")
    # calculate_turn_stats_for_player(ps, cursor, player_id) # A ser implementado
    
    # print(f"  Calculando stats de River para {player_name}...")
    # calculate_river_stats_for_player(ps, cursor, player_id) # A ser implementado

    # Adicione aqui quaisquer cálculos de stats que cruzam streets ou são gerais após os de street
    
    return ps