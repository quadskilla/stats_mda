# hand_parser.py
import re
from collections import defaultdict

# Regex (copiadas do poker_parser.py original)
RE_HAND_HEADER = re.compile(
    r"PokerStars Hand #(\d+): Tournament #(\d+),"
    r".*?"
    r"(?:- Match Round .*?,)?\s*Level\s+.*?"
    r" - "
    r"(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} \w+)"
)
RE_TABLE_INFO = re.compile(r"Table '(\d+) (\d+)' (\d+)-max Seat #(\d+) is the button")
RE_SEAT_INFO = re.compile(r"Seat (\d+): (.*?) \((\d+) in chips(?:, \$([\d\.]+) bounty)?\)")
RE_ANTE = re.compile(r"^(.*?): posts the ante (\d+)")
RE_SB = re.compile(r"^(.*?): posts small blind (\d+)")
RE_BB = re.compile(r"^(.*?): posts big blind (\d+)")
RE_DEALT_TO = re.compile(r"Dealt to (.*?) \[(.*?)\]")
RE_ACTION_FOLDS = re.compile(r"^(.*?): folds")
RE_ACTION_CHECKS = re.compile(r"^(.*?): checks")
RE_ACTION_CALLS = re.compile(r"^(.*?): calls (\d+)(?: and is all-in)?")
RE_ACTION_BETS = re.compile(r"^(.*?): bets (\d+)(?: and is all-in)?")
RE_ACTION_RAISES = re.compile(r"^(.*?): raises (\d+) to (\d+)(?: and is all-in)?")
RE_UNCALLED_BET = re.compile(r"Uncalled bet \((\d+)\) returned to (.*)")
RE_COLLECTED_POT = re.compile(r"^(.*?): collected (\d+) from pot")
RE_SHOWS_HAND = re.compile(r"^(.*?): shows \[(.*?)\](?: \((.*?)\))?")
RE_DOESNT_SHOW = re.compile(r"^(.*?): doesn't show hand")
RE_MUCKS_HAND = re.compile(r"^(.*?): mucks hand")
RE_BOARD_CARDS = re.compile(r"Board \[(.*?)\]")


POSITION_NAMES_ORDERED = {
    2: [], 3: [], 4: ["UTG"], 5: ["UTG", "CO"], 6: ["UTG", "MP", "CO"],
    7: ["UTG", "UTG+1", "MP", "CO"], 8: ["UTG", "UTG+1", "LJ", "HJ", "CO"],
    9: ["UTG", "UTG+1", "UTG+2", "LJ", "HJ", "CO"],
    10: ["UTG", "UTG+1", "UTG+2", "MP1", "LJ", "HJ", "CO"],
}

def assign_player_positions(player_seat_info, button_seat_num):
    # ... (COPIE SUA FUNÇÃO assign_player_positions AQUI do poker_parser.py)
    active_seats = sorted([seat for seat, info in player_seat_info.items() if info['name'] is not None and info.get('chips', 0) > 0])
    num_players = len(active_seats)
    if num_players == 0: return {}
    player_positions = {}
    try:
        button_idx_in_active = active_seats.index(button_seat_num)
    except ValueError:
        if not active_seats: return {}
        found_valid_button = False
        for seat_candidate in active_seats:
            if player_seat_info.get(seat_candidate) and player_seat_info[seat_candidate]['name']:
                button_seat_num = seat_candidate
                button_idx_in_active = active_seats.index(button_seat_num)
                found_valid_button = True
                break
        if not found_valid_button:
            if active_seats:
                potential_btn_seat = active_seats[0]
                if player_seat_info.get(potential_btn_seat) and player_seat_info[potential_btn_seat]['name']:
                    button_seat_num = potential_btn_seat
                    button_idx_in_active = 0 
                else:
                    return {} 
            else:
                return {}

    if not player_seat_info.get(button_seat_num) or not player_seat_info[button_seat_num]['name']:
        # Esta condição é menos provável de ser atingida devido ao fallback acima.
        # Se ocorrer, significa que o assento do botão especificado está vazio e nenhum fallback funcionou.
        return {}


    btn_player_name = player_seat_info[button_seat_num]['name']
    player_positions[btn_player_name] = "BTN"

    if num_players == 2: 
        bb_idx = (button_idx_in_active + 1) % num_players
        bb_seat = active_seats[bb_idx]
        if player_seat_info.get(bb_seat) and player_seat_info[bb_seat]['name']:
            bb_player_name = player_seat_info[bb_seat]['name']
            player_positions[bb_player_name] = "BB"
    elif num_players > 2:
        sb_idx = (button_idx_in_active + 1) % num_players
        sb_seat = active_seats[sb_idx]
        if player_seat_info.get(sb_seat) and player_seat_info[sb_seat]['name']:
            sb_player_name = player_seat_info[sb_seat]['name']
            player_positions[sb_player_name] = "SB"

        bb_idx = (button_idx_in_active + 2) % num_players
        bb_seat = active_seats[bb_idx]
        if player_seat_info.get(bb_seat) and player_seat_info[bb_seat]['name']:
            bb_player_name = player_seat_info[bb_seat]['name']
            player_positions[bb_player_name] = "BB"
        
        position_names_for_table = []
        if num_players == 3: pass 
        elif num_players == 4: position_names_for_table = ["UTG"] 
        elif num_players >= 5:
            ordered_pos_names = POSITION_NAMES_ORDERED.get(num_players, [])
            position_names_for_table = ordered_pos_names 

        current_idx_in_active = (bb_idx + 1) % num_players
        for pos_name_idx, pos_name in enumerate(position_names_for_table):
            if not active_seats: break 
            player_seat_to_assign = active_seats[current_idx_in_active]
            if player_seat_to_assign == button_seat_num: 
                break 
            if player_seat_info.get(player_seat_to_assign) and player_seat_info[player_seat_to_assign]['name']:
                player_name_to_assign = player_seat_info[player_seat_to_assign]['name']
                if player_name_to_assign not in player_positions: 
                     player_positions[player_name_to_assign] = pos_name
            current_idx_in_active = (current_idx_in_active + 1) % num_players
            if current_idx_in_active == button_idx_in_active : 
                break
    return player_positions


class PokerHand:
    # ... (COPIE A CLASSE PokerHand INTEIRA AQUI do poker_parser.py,
    #      MAS REMOVA o método save_to_db, pois ele estará em db_manager.py)
    def __init__(self, hand_id, tournament_id, datetime_str, table_id, button_seat_num):
        self.hand_id = hand_id
        self.tournament_id = tournament_id
        self.datetime_str = datetime_str
        self.table_id = table_id
        self.button_seat_num = button_seat_num
        self.player_seat_info = defaultdict(lambda: {'name': None, 'chips': 0, 'bounty': None})
        self.player_positions = {}
        self.hero_name = None
        self.actions = [] 

        self.preflop_aggressor = None
        self.flop_aggressor = None
        self.turn_aggressor = None
        self.river_aggressor = None 

        self.streets_seen = set() 
        self.hole_cards = {} 
        self.board_cards = [] 
        self.preflop_raise_count = 0
        self.first_raiser_preflop = None 

        self.flop_actors_in_order = []
        self.turn_actors_in_order = []
        self.river_actors_in_order = []

        self.current_pot_total = 0
        self.big_blind_amount = 0 
        self.pot_total_at_start_of_street = {"Preflop": 0, "Flop": 0, "Turn": 0, "River": 0}
        self.amount_to_call_overall_this_street = 0 
        self.last_bet_or_raise_amount_this_street = 0 
        self.pot_before_last_bet_or_raise_this_street = 0 
        self.bets_this_street_by_player = defaultdict(int) 
        self.current_street_aggressor = None 


    def _reset_street_betting_state(self, street_name):
        self.pot_total_at_start_of_street[street_name] = self.current_pot_total
        self.amount_to_call_overall_this_street = 0
        self.last_bet_or_raise_amount_this_street = 0
        self.pot_before_last_bet_or_raise_this_street = self.current_pot_total 
        self.bets_this_street_by_player.clear()

        if street_name == "Flop":
            self.current_street_aggressor = self.preflop_aggressor
        elif street_name == "Turn":
            self.current_street_aggressor = self.flop_aggressor
        elif street_name == "River":
            self.current_street_aggressor = self.turn_aggressor

    def add_action(self, action_data):
        player_name = action_data.get('player')
        action_type = action_data.get('action')
        amount = action_data.get('amount', 0) if action_data.get('amount') is not None else 0
        street = action_data.get('street')

        action_data['pot_total_before_action'] = self.current_pot_total
        amount_player_already_invested_this_street = self.bets_this_street_by_player.get(player_name, 0)
        action_data['amount_to_call_for_player'] = max(0, self.amount_to_call_overall_this_street - amount_player_already_invested_this_street)

        if action_data['amount_to_call_for_player'] > 0: 
            action_data['bet_faced_by_player_amount'] = self.last_bet_or_raise_amount_this_street
            action_data['pot_when_bet_was_made'] = self.pot_before_last_bet_or_raise_this_street
        else: 
            action_data['bet_faced_by_player_amount'] = 0
            action_data['pot_when_bet_was_made'] = self.current_pot_total 

        self.actions.append(action_data) 

        if action_type == 'posts_ante':
            self.current_pot_total += amount
            self.pot_total_at_start_of_street["Preflop"] += amount 
        elif action_type == 'posts_sb':
            self.current_pot_total += amount
            self.bets_this_street_by_player[player_name] += amount
            self.pot_total_at_start_of_street["Preflop"] += amount
            self.amount_to_call_overall_this_street = max(self.amount_to_call_overall_this_street, amount)
        elif action_type == 'posts_bb':
            self.current_pot_total += amount
            self.bets_this_street_by_player[player_name] += amount
            self.big_blind_amount = amount 
            self.pot_total_at_start_of_street["Preflop"] += amount
            self.pot_before_last_bet_or_raise_this_street = self.current_pot_total - amount 
            self.last_bet_or_raise_amount_this_street = amount 
            self.amount_to_call_overall_this_street = max(self.amount_to_call_overall_this_street, amount)
            self.current_street_aggressor = player_name 
        elif action_type == 'calls':
            amount_called = amount 
            self.current_pot_total += amount_called
            self.bets_this_street_by_player[player_name] += amount_called
        elif action_type == 'bets':
            self.pot_before_last_bet_or_raise_this_street = self.current_pot_total 
            self.current_pot_total += amount
            self.bets_this_street_by_player[player_name] += amount
            self.amount_to_call_overall_this_street = amount 
            self.last_bet_or_raise_amount_this_street = amount 
            self.current_street_aggressor = player_name 
            if street == "Preflop": self.preflop_aggressor = player_name 
        elif action_type == 'raises':
            total_bet_this_action = action_data['total_bet']
            money_added_by_raiser = total_bet_this_action - self.bets_this_street_by_player.get(player_name, 0)
            self.pot_before_last_bet_or_raise_this_street = self.current_pot_total 
            self.current_pot_total += money_added_by_raiser
            self.bets_this_street_by_player[player_name] = total_bet_this_action 
            self.amount_to_call_overall_this_street = total_bet_this_action 
            self.last_bet_or_raise_amount_this_street = action_data['amount'] 
            self.current_street_aggressor = player_name 
            if street == "Preflop": self.preflop_aggressor = player_name 
        elif action_type == 'uncalled_bet_returned':
            self.current_pot_total -= amount 
        if action_data['street'] and action_data['street'] not in ["Pre-deal", "Summary", "Showdown"]:
            self.streets_seen.add(action_data['street'])
        if action_data['street'] == 'Preflop' and action_type in ['bets', 'raises']: 
            self.preflop_raise_count += 1
            if self.preflop_raise_count == 1 and not self.first_raiser_preflop:
                self.first_raiser_preflop = player_name

    def _determine_street_actors_order(self, street_name, target_list):
        target_list.clear()
        seen_actors = set()
        relevant_actions = ['bets', 'raises', 'calls', 'checks', 'folds']
        street_actions_only = [a for a in self.actions if a['street'] == street_name and a['action'] in relevant_actions and a.get('player') is not None]
        for action in street_actions_only:
            if action['player'] not in seen_actors:
                target_list.append(action['player'])
                seen_actors.add(action['player'])

    def determine_actors_order(self):
        self._determine_street_actors_order("Flop", self.flop_actors_in_order)
        self._determine_street_actors_order("Turn", self.turn_actors_in_order)
        self._determine_street_actors_order("River", self.river_actors_in_order)

    def is_player_ip_on_street(self, player_name, street_aggressor_for_comparison, street_actors_order, street_name_param=None): # Renomeado street_name para street_name_param
        if not street_actors_order or player_name not in street_actors_order: return None
        try:
            player_idx = street_actors_order.index(player_name)
        except ValueError:
            return None 
        if street_aggressor_for_comparison and street_aggressor_for_comparison in street_actors_order:
            try:
                aggressor_idx = street_actors_order.index(street_aggressor_for_comparison)
                if player_name == street_aggressor_for_comparison:
                    active_opp_indices = []
                    for opp_cand_name in street_actors_order:
                        if opp_cand_name != player_name:
                            opp_actions_this_street = [
                                a for a in self.actions
                                if a.get('street') == street_name_param and a.get('player') == opp_cand_name # Usar street_name_param
                            ]
                            if not any(a.get('action') == 'folds' for a in opp_actions_this_street):
                                if opp_cand_name in street_actors_order: 
                                   active_opp_indices.append(street_actors_order.index(opp_cand_name))
                    if not active_opp_indices: return True 
                    return player_idx > max(active_opp_indices)
                else:
                    return player_idx > aggressor_idx
            except ValueError:
                pass 
        active_player_indices_this_street = []
        for p_name_in_order in street_actors_order:
            p_actions_this_street = [
                a for a in self.actions
                if a.get('street') == street_name_param and a.get('player') == p_name_in_order # Usar street_name_param
            ]
            if not any(a.get('action') == 'folds' for a in p_actions_this_street):
                if p_name_in_order in street_actors_order: 
                    active_player_indices_this_street.append(street_actors_order.index(p_name_in_order))
        if not active_player_indices_this_street: return None 
        return player_idx == max(active_player_indices_this_street)

    def is_player_oop_to_another(self, player_name, other_player_name, street_actors_order):
        if not street_actors_order or player_name not in street_actors_order or other_player_name not in street_actors_order:
            return None 
        if player_name == other_player_name: return False 
        try:
            player_idx = street_actors_order.index(player_name)
            other_player_idx = street_actors_order.index(other_player_name)
            return player_idx < other_player_idx 
        except ValueError:
            return None 

    def set_hero(self, player_name): self.hero_name = player_name
    def set_hole_cards(self, player_name, cards): self.hole_cards[player_name] = cards
    def get_player_position(self, player_name): return self.player_positions.get(player_name)
    def __repr__(self):
        return (f"<PokerHand ID: {self.hand_id}, Pote Final: {self.current_pot_total}, "
                f"PFA: {self.preflop_aggressor}, FA: {self.flop_aggressor}, "
                f"TA: {self.turn_aggressor}, RA: {self.river_aggressor}, "
                f"Ações: {len(self.actions)}>")


def parse_hand_history_to_object(hand_text_block):
    # ... (COPIE SUA FUNÇÃO parse_hand_history_to_object AQUI do poker_parser.py)
    #    (Ela deve criar e retornar um objeto PokerHand)
    lines = hand_text_block.strip().split('\n')
    if not lines: return None
    first_line = lines[0]
    m_header = RE_HAND_HEADER.match(first_line)
    if not m_header: return None
    hand_id, tournament_id, datetime_str = m_header.groups()
    
    current_hand = PokerHand(hand_id, tournament_id, datetime_str, None, None)
    current_street = "Pre-deal"
    positions_assigned_for_hand = False

    for line_idx, line_content in enumerate(lines[1:], start=1):
        line = line_content.strip()
        if not line: continue

        new_street_detected = None
        old_street = current_street

        if line.startswith("*** HOLE CARDS ***"): new_street_detected = "Preflop"
        elif line.startswith("*** FLOP ***"): new_street_detected = "Flop"
        elif line.startswith("*** TURN ***"): new_street_detected = "Turn"
        elif line.startswith("*** RIVER ***"): new_street_detected = "River"
        elif line.startswith("*** SHOW DOWN ***"): new_street_detected = "Showdown"
        elif line.startswith("*** SUMMARY ***"): new_street_detected = "Summary"

        if new_street_detected and new_street_detected != current_street:
            if old_street == "Preflop": current_hand.preflop_aggressor = current_hand.current_street_aggressor
            elif old_street == "Flop": current_hand.flop_aggressor = current_hand.current_street_aggressor
            elif old_street == "Turn": current_hand.turn_aggressor = current_hand.current_street_aggressor
            current_street = new_street_detected
            if current_street in ["Flop", "Turn", "River"]:
                current_hand._reset_street_betting_state(current_street)
            if current_street == "Preflop" and current_hand.button_seat_num is not None and not positions_assigned_for_hand:
                valid_player_seat_info = {s:i for s,i in current_hand.player_seat_info.items() if i['name'] is not None and i.get('chips',0) > 0}
                if valid_player_seat_info:
                     current_hand.player_positions = assign_player_positions(valid_player_seat_info, current_hand.button_seat_num)
                     positions_assigned_for_hand = True
            if current_street == "Summary": 
                m_board_search = RE_BOARD_CARDS.search(hand_text_block)
                if m_board_search:
                    current_hand.board_cards = m_board_search.group(1).split(' ')
            continue 
        m = RE_TABLE_INFO.match(line)
        if m:
            current_hand.table_id = m.group(1) 
            current_hand.button_seat_num = int(m.group(4))
            continue
        m = RE_SEAT_INFO.match(line)
        if m:
            seat, player_name, chips, bounty_str = int(m.group(1)), m.group(2), int(m.group(3)), m.group(4)
            current_hand.player_seat_info[seat]['name'] = player_name
            current_hand.player_seat_info[seat]['chips'] = int(chips)
            if bounty_str:
                 current_hand.player_seat_info[seat]['bounty'] = float(bounty_str)
            continue
        m = RE_DEALT_TO.match(line)
        if m:
            hero_name, cards = m.groups()
            current_hand.set_hero(hero_name)
            current_hand.set_hole_cards(hero_name, cards)
            continue
        if not positions_assigned_for_hand and current_hand.button_seat_num is not None and \
           current_street not in ["Summary", "Pre-deal", "Showdown"] and \
           any((info.get('name') or "") + ": " in line for seat, info in current_hand.player_seat_info.items()): 
            valid_player_seat_info = {s:i for s,i in current_hand.player_seat_info.items() if i['name'] is not None and i.get('chips',0) > 0}
            if valid_player_seat_info:
                 current_hand.player_positions = assign_player_positions(valid_player_seat_info, current_hand.button_seat_num)
                 positions_assigned_for_hand = True
        action_data = {
            'hand_id': current_hand.hand_id, 'street': current_street, 'player': None,
            'action': None, 'amount': None, 'total_bet': None,
            'position': "N/A", 'hero': False, 'description': None
        }
        player_name_from_action = None
        action_parsed = False
        m = RE_ANTE.match(line);
        if m: player_name_from_action, amount_str = m.groups(); action_data.update({'action': 'posts_ante', 'amount': int(amount_str)}); action_parsed = True
        elif RE_SB.match(line): m = RE_SB.match(line); player_name_from_action, amount_str = m.groups(); action_data.update({'action': 'posts_sb', 'amount': int(amount_str)}); action_parsed = True
        elif RE_BB.match(line): m = RE_BB.match(line); player_name_from_action, amount_str = m.groups(); action_data.update({'action': 'posts_bb', 'amount': int(amount_str)}); action_parsed = True
        elif RE_ACTION_FOLDS.match(line): m = RE_ACTION_FOLDS.match(line); player_name_from_action = m.group(1); action_data.update({'action': 'folds'}); action_parsed = True
        elif RE_ACTION_CHECKS.match(line): m = RE_ACTION_CHECKS.match(line); player_name_from_action = m.group(1); action_data.update({'action': 'checks'}); action_parsed = True
        elif RE_ACTION_CALLS.match(line): m = RE_ACTION_CALLS.match(line); player_name_from_action, amount_str = m.groups()[:2]; action_data.update({'action': 'calls', 'amount': int(amount_str)}); action_parsed = True
        elif RE_ACTION_BETS.match(line): m = RE_ACTION_BETS.match(line); player_name_from_action, amount_str = m.groups()[:2]; action_data.update({'action': 'bets', 'amount': int(amount_str)}); action_parsed = True
        elif RE_ACTION_RAISES.match(line): m = RE_ACTION_RAISES.match(line); player_name_from_action, amount_str, total_bet_str = m.groups()[:3]; action_data.update({'action': 'raises', 'amount': int(amount_str), 'total_bet': int(total_bet_str)}); action_parsed = True
        elif RE_UNCALLED_BET.match(line): m = RE_UNCALLED_BET.match(line); amount_str, player_name_from_action = m.groups(); action_data.update({'action': 'uncalled_bet_returned', 'amount': int(amount_str)}); action_parsed = True
        elif RE_COLLECTED_POT.match(line): m = RE_COLLECTED_POT.match(line); player_name_from_action, amount_str = m.groups(); action_data.update({'action': 'collected_pot', 'amount': int(amount_str)}); action_parsed = True
        elif RE_SHOWS_HAND.match(line):
            m = RE_SHOWS_HAND.match(line); player_name_from_action, cards_shown, description_shown = m.groups()
            action_data.update({'action': 'shows_hand', 'cards': cards_shown, 'description': description_shown})
            if player_name_from_action: current_hand.set_hole_cards(player_name_from_action, cards_shown) 
            action_parsed = True
        elif RE_DOESNT_SHOW.match(line): m = RE_DOESNT_SHOW.match(line); player_name_from_action = m.group(1); action_data.update({'action': 'doesnt_show_hand'}); action_parsed = True
        elif RE_MUCKS_HAND.match(line): m = RE_MUCKS_HAND.match(line); player_name_from_action = m.group(1); action_data.update({'action': 'mucks_hand'}); action_parsed = True
        m_board = RE_BOARD_CARDS.match(line)
        if m_board:
            current_hand.board_cards = m_board.group(1).split(' ')
            continue 
        if action_parsed and player_name_from_action:
            action_data['player'] = player_name_from_action
            pos = current_hand.player_positions.get(player_name_from_action, "N/A_NoPosYet")
            action_data['position'] = pos
            if current_hand.hero_name and player_name_from_action == current_hand.hero_name:
                action_data['hero'] = True
            if action_data['action'] and action_data['player'] and current_street not in ["Summary", None, "Pre-deal", "Showdown"]:
                 current_hand.add_action(action_data)
            elif action_data['action'] in ['posts_ante', 'posts_sb', 'posts_bb'] and current_street == "Pre-deal":
                current_hand.add_action(action_data) 
            elif action_data['action'] in ['shows_hand', 'mucks_hand', 'doesnt_show_hand', 'collected_pot', 'uncalled_bet_returned'] and current_street in ["Showdown", "Summary"]:
                 current_hand.actions.append(action_data) 
    if current_street == "River": current_hand.river_aggressor = current_hand.current_street_aggressor
    elif current_street == "Turn" and not current_hand.streets_seen.intersection({"River", "Showdown", "Summary"}):
        current_hand.turn_aggressor = current_hand.current_street_aggressor
    elif current_street == "Flop" and not current_hand.streets_seen.intersection({"Turn", "River", "Showdown", "Summary"}):
        current_hand.flop_aggressor = current_hand.current_street_aggressor
    if current_hand:
        current_hand.determine_actors_order() 
    return current_hand