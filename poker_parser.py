import os
import re
from collections import defaultdict
import math
import pickle
import sqlite3

# --- Configuração do Banco de Dados ---
DB_NAME = "poker_data.db"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row # Para acessar colunas por nome
    # Habilitar foreign keys constraint (bom para integridade)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def create_tables(conn):
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS players (
        player_id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT UNIQUE NOT NULL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS hands (
        hand_db_id INTEGER PRIMARY KEY AUTOINCREMENT,
        hand_history_id TEXT UNIQUE NOT NULL,
        tournament_id TEXT,
        datetime_str TEXT,
        table_id TEXT,
        button_seat_num INTEGER,
        hero_id INTEGER,
        big_blind_amount INTEGER,
        board_cards TEXT,
        preflop_aggressor_id INTEGER,
        flop_aggressor_id INTEGER,
        turn_aggressor_id INTEGER,
        river_aggressor_id INTEGER,
        pot_total_at_showdown INTEGER,
        FOREIGN KEY (hero_id) REFERENCES players(player_id) ON DELETE SET NULL,
        FOREIGN KEY (preflop_aggressor_id) REFERENCES players(player_id) ON DELETE SET NULL,
        FOREIGN KEY (flop_aggressor_id) REFERENCES players(player_id) ON DELETE SET NULL,
        FOREIGN KEY (turn_aggressor_id) REFERENCES players(player_id) ON DELETE SET NULL,
        FOREIGN KEY (river_aggressor_id) REFERENCES players(player_id) ON DELETE SET NULL
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS hand_players (
        hand_player_id INTEGER PRIMARY KEY AUTOINCREMENT,
        hand_db_id INTEGER NOT NULL,
        player_id INTEGER NOT NULL,
        seat_num INTEGER,
        initial_chips INTEGER,
        position TEXT,
        hole_cards TEXT,
        FOREIGN KEY (hand_db_id) REFERENCES hands(hand_db_id) ON DELETE CASCADE,
        FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE,
        UNIQUE (hand_db_id, player_id)
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS actions (
        action_id INTEGER PRIMARY KEY AUTOINCREMENT,
        hand_db_id INTEGER NOT NULL,
        player_id INTEGER,
        street TEXT,
        action_type TEXT NOT NULL,
        amount INTEGER,
        total_bet_amount INTEGER,
        action_sequence INTEGER NOT NULL,
        pot_total_before_action INTEGER,
        amount_to_call_for_player INTEGER,
        bet_faced_by_player_amount INTEGER,
        pot_when_bet_was_made INTEGER,
        FOREIGN KEY (hand_db_id) REFERENCES hands(hand_db_id) ON DELETE CASCADE,
        FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE SET NULL
    )
    """)
    conn.commit()

def get_or_create_player_id(conn, player_name):
    if not player_name:
        return None
    cursor = conn.cursor()
    cursor.execute("SELECT player_id FROM players WHERE player_name = ?", (player_name,))
    row = cursor.fetchone()
    if row:
        return row['player_id']
    else:
        try:
            cursor.execute("INSERT INTO players (player_name) VALUES (?)", (player_name,))
            # conn.commit() # Commit é feito em lote ou no final de save_to_db
            return cursor.lastrowid
        except sqlite3.IntegrityError: # Raro, mas caso haja concorrência ou se o commit não for imediato
            cursor.execute("SELECT player_id FROM players WHERE player_name = ?", (player_name,))
            row = cursor.fetchone()
            return row['player_id'] if row else None


# --- Funções Auxiliares para Posições ---
POSITION_NAMES_ORDERED = {
    2: [], 3: [], 4: ["UTG"], 5: ["UTG", "CO"], 6: ["UTG", "MP", "CO"],
    7: ["UTG", "UTG+1", "MP", "CO"], 8: ["UTG", "UTG+1", "LJ", "HJ", "CO"],
    9: ["UTG", "UTG+1", "UTG+2", "LJ", "HJ", "CO"],
    10: ["UTG", "UTG+1", "UTG+2", "MP1", "LJ", "HJ", "CO"],
}
def assign_player_positions(player_seat_info, button_seat_num):
    active_seats = sorted([seat for seat, info in player_seat_info.items() if info['name'] is not None and info.get('chips', 0) > 0])
    num_players = len(active_seats)
    if num_players == 0: return {}
    player_positions = {}
    try:
        button_idx_in_active = active_seats.index(button_seat_num)
    except ValueError:
        if not active_seats: return {}
        # Tenta encontrar um botão válido se o fornecido não estiver ativo
        found_valid_button = False
        for seat_candidate in active_seats:
            if player_seat_info.get(seat_candidate) and player_seat_info[seat_candidate]['name']:
                button_seat_num = seat_candidate
                button_idx_in_active = active_seats.index(button_seat_num)
                found_valid_button = True
                break
        if not found_valid_button: # Se nenhum jogador ativo pode ser o botão (ex: todos sentaram)
             # Fallback: tentar pegar o primeiro jogador ativo como botão se nenhum botão foi encontrado antes
            if active_seats:
                potential_btn_seat = active_seats[0]
                if player_seat_info.get(potential_btn_seat) and player_seat_info[potential_btn_seat]['name']:
                    button_seat_num = potential_btn_seat
                    button_idx_in_active = 0 # Definir como 0 se é o primeiro da lista
                else:
                    return {} # Não foi possível determinar um botão válido
            else:
                return {}


    if not player_seat_info.get(button_seat_num) or not player_seat_info[button_seat_num]['name']:
        found_btn = False
        for seat_idx in active_seats: # Tenta encontrar um jogador ativo no botão
            if player_seat_info.get(seat_idx) and player_seat_info[seat_idx]['name']:
                # Este é um cenário onde o button_seat_num original pode não ter um jogador
                # mas o número do assento do botão é fixo. Devemos procurar quem está nesse assento.
                # A lógica original de encontrar o botão em active_seats é melhor.
                pass # A lógica acima com active_seats.index(button_seat_num) já deve cobrir.
        # Se ainda não encontrou, pode ser um problema.
        # Vamos assumir que button_seat_num sempre terá um jogador ativo se num_players > 0,
        # devido à lógica de fallback acima.
        if not active_seats: return {} # Adicionado para cobrir caso extremo


    btn_player_name = player_seat_info[button_seat_num]['name']
    player_positions[btn_player_name] = "BTN"

    if num_players == 2: # Heads-up
        # O botão é SB, o outro é BB
        bb_idx = (button_idx_in_active + 1) % num_players
        bb_seat = active_seats[bb_idx]
        if player_seat_info.get(bb_seat) and player_seat_info[bb_seat]['name']:
            bb_player_name = player_seat_info[bb_seat]['name']
            player_positions[bb_player_name] = "BB"
            # Em HU, o BTN também é o SB. Pode ser redundante se o "SB" for atribuído depois.
            # Se a sua lógica de stats precisa de "SB" explicitamente para HU BTN, adicione aqui.
            # Caso contrário, "BTN" já identifica o primeiro a agir pré-flop.
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

        # Posições restantes
        position_names_for_table = []
        if num_players == 3: pass # UTG não existe em 3-handed (BTN, SB, BB)
        elif num_players == 4: position_names_for_table = ["UTG"] # PokerStars chama de UTG, outros de CO
        elif num_players >= 5:
            # Ajustar para pegar o array correto de POSITION_NAMES_ORDERED
            # O array em POSITION_NAMES_ORDERED é para jogadores EP/MP/LP *antes* do CO/BTN/SB/BB
            # então precisamos dos primeiros num_players - 3 (BTN,SB,BB) ou num_players - 2 (BTN, BB em HU)
            # A chave de POSITION_NAMES_ORDERED é o número total de jogadores
            ordered_pos_names = POSITION_NAMES_ORDERED.get(num_players, [])
            # O número de posições "early" e "middle" a serem preenchidas é num_players - 3 (para BTN, SB, BB)
            # Se for HU (num_players=2), não há posições além de BTN/BB.
            # Se for 3-handed, também não há.
            # Se for 4-handed, só há UTG.
            # A lista em POSITION_NAMES_ORDERED é para as posições que vêm DEPOIS do BB e ANTES do CO.
            # Ex: 9-max: UTG, UTG+1, UTG+2, LJ, HJ, CO (6 posições) + BTN, SB, BB
            # O loop abaixo começa após o BB.
            position_names_for_table = ordered_pos_names # Usa a lista completa para o número de jogadores

        current_idx_in_active = (bb_idx + 1) % num_players
        for pos_name_idx, pos_name in enumerate(position_names_for_table):
            if not active_seats: break # Segurança
            player_seat_to_assign = active_seats[current_idx_in_active]

            # Parar se voltarmos ao botão (significa que todas as posições foram atribuídas)
            if player_seat_to_assign == button_seat_num: #ou sb_seat ou bb_seat se já atribuídos
                break # Todas as posições antes do CO/BTN/SB/BB devem ter sido preenchidas

            if player_seat_info.get(player_seat_to_assign) and player_seat_info[player_seat_to_assign]['name']:
                player_name_to_assign = player_seat_info[player_seat_to_assign]['name']
                if player_name_to_assign not in player_positions: # Não sobrescrever BTN, SB, BB
                     player_positions[player_name_to_assign] = pos_name
            current_idx_in_active = (current_idx_in_active + 1) % num_players
            if current_idx_in_active == button_idx_in_active : # Evitar loop infinito se algo der errado
                break
    return player_positions


# --- Regex para parsing (sem alterações) ---
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


class PokerHand:
    def __init__(self, hand_id, tournament_id, datetime_str, table_id, button_seat_num):
        self.hand_id = hand_id
        self.tournament_id = tournament_id
        self.datetime_str = datetime_str
        self.table_id = table_id
        self.button_seat_num = button_seat_num
        self.player_seat_info = defaultdict(lambda: {'name': None, 'chips': 0, 'bounty': None})
        self.player_positions = {}
        self.hero_name = None
        self.actions = [] # Lista de dicts representando cada ação

        # Agressores da street (nome do jogador)
        self.preflop_aggressor = None
        self.flop_aggressor = None
        self.turn_aggressor = None
        self.river_aggressor = None # Último agressor no river

        self.streets_seen = set() # Quais streets foram vistas (Flop, Turn, River)
        self.hole_cards = {} # player_name: cards_string
        self.board_cards = [] # Lista de strings de cartas
        self.preflop_raise_count = 0
        self.first_raiser_preflop = None # Quem fez o primeiro raise pré-flop

        # Para rastrear ordem de ação em cada street
        self.flop_actors_in_order = []
        self.turn_actors_in_order = []
        self.river_actors_in_order = []

        # Estado do pote e apostas para cálculo de size
        self.current_pot_total = 0
        self.big_blind_amount = 0 # Valor do BB na mão
        self.pot_total_at_start_of_street = {"Preflop": 0, "Flop": 0, "Turn": 0, "River": 0}
        self.amount_to_call_overall_this_street = 0 # Quanto é preciso para dar call na aposta/raise atual
        self.last_bet_or_raise_amount_this_street = 0 # O valor do último bet/raise (ex: raise 500 to 700, aqui é 500)
        self.pot_before_last_bet_or_raise_this_street = 0 # Pote antes do último bet/raise
        self.bets_this_street_by_player = defaultdict(int) # Quanto cada jogador já investiu nesta street
        self.current_street_aggressor = None # Quem fez a última ação agressiva na street atual


    def _reset_street_betting_state(self, street_name):
        self.pot_total_at_start_of_street[street_name] = self.current_pot_total
        self.amount_to_call_overall_this_street = 0
        self.last_bet_or_raise_amount_this_street = 0
        self.pot_before_last_bet_or_raise_this_street = self.current_pot_total # Pote antes de qualquer ação na nova street
        self.bets_this_street_by_player.clear()

        # O agressor da street anterior carrega a iniciativa
        if street_name == "Flop":
            self.current_street_aggressor = self.preflop_aggressor
        elif street_name == "Turn":
            self.current_street_aggressor = self.flop_aggressor
        elif street_name == "River":
            self.current_street_aggressor = self.turn_aggressor
        # Se não houver agressor anterior (ex: todos deram check), current_street_aggressor é None

    def add_action(self, action_data):
        player_name = action_data.get('player')
        action_type = action_data.get('action')
        amount = action_data.get('amount', 0) if action_data.get('amount') is not None else 0
        street = action_data.get('street')

        # Salvar estado do pote ANTES desta ação para referência
        action_data['pot_total_before_action'] = self.current_pot_total
        amount_player_already_invested_this_street = self.bets_this_street_by_player.get(player_name, 0)
        action_data['amount_to_call_for_player'] = max(0, self.amount_to_call_overall_this_street - amount_player_already_invested_this_street)

        if action_data['amount_to_call_for_player'] > 0: # Jogador está enfrentando uma aposta/raise
            action_data['bet_faced_by_player_amount'] = self.last_bet_or_raise_amount_this_street
            action_data['pot_when_bet_was_made'] = self.pot_before_last_bet_or_raise_this_street
        else: # Jogador não está enfrentando aposta (pode checkar ou betar)
            action_data['bet_faced_by_player_amount'] = 0
            action_data['pot_when_bet_was_made'] = self.current_pot_total # Pote atual se ele for betar "do nada"

        self.actions.append(action_data) # Adiciona a ação à lista da mão

        # Atualizar o estado do pote e apostas COM BASE na ação
        if action_type == 'posts_ante':
            self.current_pot_total += amount
            self.pot_total_at_start_of_street["Preflop"] += amount # Antes de qualquer street
        elif action_type == 'posts_sb':
            self.current_pot_total += amount
            self.bets_this_street_by_player[player_name] += amount
            self.pot_total_at_start_of_street["Preflop"] += amount
            self.amount_to_call_overall_this_street = max(self.amount_to_call_overall_this_street, amount)
            # SB não é considerado o "agressor" inicial normalmente, BB sim.
        elif action_type == 'posts_bb':
            self.current_pot_total += amount
            self.bets_this_street_by_player[player_name] += amount
            self.big_blind_amount = amount # Define o BB da mão
            self.pot_total_at_start_of_street["Preflop"] += amount
            self.pot_before_last_bet_or_raise_this_street = self.current_pot_total - amount # Pote antes do BB
            self.last_bet_or_raise_amount_this_street = amount # O BB é a "aposta" a ser paga
            self.amount_to_call_overall_this_street = max(self.amount_to_call_overall_this_street, amount)
            self.current_street_aggressor = player_name # BB é o primeiro "agressor" implícito

        elif action_type == 'calls':
            amount_called = amount # O 'amount' em 'calls' é o valor adicional pago
            self.current_pot_total += amount_called
            self.bets_this_street_by_player[player_name] += amount_called
            # Não muda o agressor
        elif action_type == 'bets':
            self.pot_before_last_bet_or_raise_this_street = self.current_pot_total # Pote antes DESTE bet
            self.current_pot_total += amount
            self.bets_this_street_by_player[player_name] += amount
            self.amount_to_call_overall_this_street = amount # Novo valor a ser pago pelos outros
            self.last_bet_or_raise_amount_this_street = amount # Valor do bet
            self.current_street_aggressor = player_name # Novo agressor
            if street == "Preflop": self.preflop_aggressor = player_name # Atualiza PFA se for bet PF (open limp não é PFA)

        elif action_type == 'raises':
            # 'amount' é o valor do raise (ex: raise 500), 'total_bet' é o valor total da aposta (ex: to 700)
            total_bet_this_action = action_data['total_bet']
            # Dinheiro efetivamente adicionado ao pote por este raiser
            money_added_by_raiser = total_bet_this_action - self.bets_this_street_by_player.get(player_name, 0)

            self.pot_before_last_bet_or_raise_this_street = self.current_pot_total # Pote antes DESTE raise
            self.current_pot_total += money_added_by_raiser
            self.bets_this_street_by_player[player_name] = total_bet_this_action # Atualiza o total investido pelo raiser
            self.amount_to_call_overall_this_street = total_bet_this_action # Novo valor total a ser pago
            self.last_bet_or_raise_amount_this_street = action_data['amount'] # O valor do raise em si

            self.current_street_aggressor = player_name # Novo agressor
            if street == "Preflop": self.preflop_aggressor = player_name # Atualiza PFA

        elif action_type == 'uncalled_bet_returned':
            self.current_pot_total -= amount # Reduz o pote
            # O agressor não muda, a aposta simplesmente não foi paga

        # Rastrear streets vistas
        if action_data['street'] and action_data['street'] not in ["Pre-deal", "Summary", "Showdown"]:
            self.streets_seen.add(action_data['street'])

        # Contar raises pré-flop e identificar o primeiro raiser
        if action_data['street'] == 'Preflop' and action_type in ['bets', 'raises']: # 'bets' aqui seria um open raise
            self.preflop_raise_count += 1
            if self.preflop_raise_count == 1 and not self.first_raiser_preflop:
                self.first_raiser_preflop = player_name


    def _determine_street_actors_order(self, street_name, target_list):
        target_list.clear()
        seen_actors = set()
        relevant_actions = ['bets', 'raises', 'calls', 'checks', 'folds']
        # Considerar apenas ações do jogador na street correta
        street_actions_only = [a for a in self.actions if a['street'] == street_name and a['action'] in relevant_actions and a.get('player') is not None]

        for action in street_actions_only:
            if action['player'] not in seen_actors:
                target_list.append(action['player'])
                seen_actors.add(action['player'])

    def determine_actors_order(self):
        self._determine_street_actors_order("Flop", self.flop_actors_in_order)
        self._determine_street_actors_order("Turn", self.turn_actors_in_order)
        self._determine_street_actors_order("River", self.river_actors_in_order)

    def is_player_ip_on_street(self, player_name, street_aggressor_for_comparison, street_actors_order, street_name=None):
        if not street_actors_order or player_name not in street_actors_order: return None

        try:
            player_idx = street_actors_order.index(player_name)
        except ValueError:
            return None # Jogador não encontrado na ordem de atores da street

        # Caso 1: Comparando com um agressor específico
        if street_aggressor_for_comparison and street_aggressor_for_comparison in street_actors_order:
            try:
                aggressor_idx = street_actors_order.index(street_aggressor_for_comparison)
                if player_name == street_aggressor_for_comparison:
                    # O agressor é IP se age depois de todos os oponentes ativos restantes
                    active_opp_indices = []
                    for opp_cand_name in street_actors_order:
                        if opp_cand_name != player_name:
                            # Verificar se oponente ainda está na mão (não foldou na street)
                            opp_actions_this_street = [
                                a for a in self.actions
                                if a.get('street') == street_name and a.get('player') == opp_cand_name
                            ]
                            if not any(a.get('action') == 'folds' for a in opp_actions_this_street):
                                if opp_cand_name in street_actors_order: # Deve estar, mas checagem dupla
                                   active_opp_indices.append(street_actors_order.index(opp_cand_name))
                    
                    if not active_opp_indices: return True # IP por padrão se não há oponentes ativos
                    return player_idx > max(active_opp_indices)
                else:
                    # Jogador é IP em relação ao agressor se age depois dele
                    return player_idx > aggressor_idx
            except ValueError:
                pass # Agressor não encontrado na ordem, passar para lógica geral

        # Caso 2: Lógica geral (jogador é IP se é o último a agir entre os jogadores ativos)
        active_player_indices_this_street = []
        for p_name_in_order in street_actors_order:
            p_actions_this_street = [
                a for a in self.actions
                if a.get('street') == street_name and a.get('player') == p_name_in_order
            ]
            if not any(a.get('action') == 'folds' for a in p_actions_this_street):
                if p_name_in_order in street_actors_order: # Deve estar
                    active_player_indices_this_street.append(street_actors_order.index(p_name_in_order))
        
        if not active_player_indices_this_street: return None # Nenhum jogador ativo? Improvável aqui.
        return player_idx == max(active_player_indices_this_street)


    def is_player_oop_to_another(self, player_name, other_player_name, street_actors_order):
        if not street_actors_order or player_name not in street_actors_order or other_player_name not in street_actors_order:
            return None # Indeterminado
        if player_name == other_player_name: return False # Não pode ser OOP a si mesmo

        try:
            player_idx = street_actors_order.index(player_name)
            other_player_idx = street_actors_order.index(other_player_name)
            return player_idx < other_player_idx # OOP se age antes do outro
        except ValueError:
            return None # Um dos jogadores não encontrado na ordem

    def set_hero(self, player_name): self.hero_name = player_name
    def set_hole_cards(self, player_name, cards): self.hole_cards[player_name] = cards
    def get_player_position(self, player_name): return self.player_positions.get(player_name)

    def __repr__(self):
        return (f"<PokerHand ID: {self.hand_id}, Pote Final: {self.current_pot_total}, "
                f"PFA: {self.preflop_aggressor}, FA: {self.flop_aggressor}, "
                f"TA: {self.turn_aggressor}, RA: {self.river_aggressor}, "
                f"Ações: {len(self.actions)}>")

    def save_to_db(self, conn):
        cursor = conn.cursor()
        player_name_to_id_map = {}
        all_player_names_in_hand = set()

        if self.hero_name: all_player_names_in_hand.add(self.hero_name)
        for seat_info in self.player_seat_info.values():
            if seat_info['name']: all_player_names_in_hand.add(seat_info['name'])
        for action_data in self.actions:
            if action_data.get('player'): all_player_names_in_hand.add(action_data['player'])
        
        # Incluir agressores na lista para garantir que seus IDs sejam criados/obtidos
        if self.preflop_aggressor: all_player_names_in_hand.add(self.preflop_aggressor)
        if self.flop_aggressor: all_player_names_in_hand.add(self.flop_aggressor)
        if self.turn_aggressor: all_player_names_in_hand.add(self.turn_aggressor)
        if self.river_aggressor: all_player_names_in_hand.add(self.river_aggressor)

        for name in filter(None, all_player_names_in_hand): # Filtra Nones se houver
            player_name_to_id_map[name] = get_or_create_player_id(conn, name)

        hero_db_id = player_name_to_id_map.get(self.hero_name)
        pfa_id = player_name_to_id_map.get(self.preflop_aggressor)
        fa_id = player_name_to_id_map.get(self.flop_aggressor)
        ta_id = player_name_to_id_map.get(self.turn_aggressor)
        ra_id = player_name_to_id_map.get(self.river_aggressor)

        final_pot_at_showdown_or_end = self.current_pot_total # Pote final da mão

        try:
            cursor.execute("""
                INSERT INTO hands (hand_history_id, tournament_id, datetime_str, table_id, button_seat_num, hero_id, big_blind_amount, board_cards,
                                 preflop_aggressor_id, flop_aggressor_id, turn_aggressor_id, river_aggressor_id, pot_total_at_showdown)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (self.hand_id, self.tournament_id, self.datetime_str, self.table_id, self.button_seat_num, hero_db_id,
                  self.big_blind_amount, ' '.join(self.board_cards) if self.board_cards else None,
                  pfa_id, fa_id, ta_id, ra_id, final_pot_at_showdown_or_end))
            hand_db_id = cursor.lastrowid
        except sqlite3.IntegrityError:
            # print(f"Mão {self.hand_id} já existe no DB. Pulando inserção detalhada.")
            # Não precisa fazer rollback, a transação da mão não começou efetivamente
            return None # Indica que a mão já existia

        for seat_num, seat_info in self.player_seat_info.items():
            if seat_info['name']:
                player_db_id = player_name_to_id_map.get(seat_info['name'])
                if player_db_id:
                    position = self.player_positions.get(seat_info['name'])
                    cards = self.hole_cards.get(seat_info['name'])
                    try:
                        cursor.execute("""
                            INSERT INTO hand_players (hand_db_id, player_id, seat_num, initial_chips, position, hole_cards)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (hand_db_id, player_db_id, seat_num, seat_info['chips'], position, cards))
                    except sqlite3.IntegrityError:
                        # print(f"Erro ao inserir {seat_info['name']} na mão {self.hand_id}. Pode já existir.")
                        pass # Pode acontecer se houver re-processamento parcial

        for i, action_data in enumerate(self.actions):
            player_name_for_action = action_data.get('player')
            player_db_id_for_action = player_name_to_id_map.get(player_name_for_action) if player_name_for_action else None
            amount_val = action_data.get('amount')
            total_bet_val = action_data.get('total_bet')
            
            # Garantir que os campos numéricos tenham valores padrão se ausentes
            pot_total_before = action_data.get('pot_total_before_action', 0)
            amount_to_call = action_data.get('amount_to_call_for_player', 0)
            bet_faced = action_data.get('bet_faced_by_player_amount', 0)
            pot_when_bet = action_data.get('pot_when_bet_was_made', 0)

            cursor.execute("""
                INSERT INTO actions (hand_db_id, player_id, street, action_type, amount, total_bet_amount, action_sequence,
                                     pot_total_before_action, amount_to_call_for_player, bet_faced_by_player_amount, pot_when_bet_was_made)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (hand_db_id, player_db_id_for_action, action_data.get('street'), action_data.get('action'),
                  amount_val, total_bet_val, i,
                  pot_total_before, amount_to_call, bet_faced, pot_when_bet
                  ))
        
        # conn.commit() # O commit será feito em parse_poker_log_file_to_hands_and_save_to_db após um lote ou no final
        return hand_db_id
    # poker_parser.py - PARTE 2

def parse_hand_history_to_object(hand_text_block): # Removido conn daqui, será passado de cima
    lines = hand_text_block.strip().split('\n')
    if not lines: return None
    first_line = lines[0]
    m_header = RE_HAND_HEADER.match(first_line)
    if not m_header: return None
    hand_id, tournament_id, datetime_str = m_header.groups()
    
    # O objeto PokerHand é criado temporariamente para coletar dados da mão
    # antes de salvar no DB.
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
            # Atualiza o agressor da street que acabou de terminar
            if old_street == "Preflop": current_hand.preflop_aggressor = current_hand.current_street_aggressor
            elif old_street == "Flop": current_hand.flop_aggressor = current_hand.current_street_aggressor
            elif old_street == "Turn": current_hand.turn_aggressor = current_hand.current_street_aggressor
            # River aggressor é o current_street_aggressor ao final do parsing da street River

            current_street = new_street_detected
            if current_street in ["Flop", "Turn", "River"]:
                current_hand._reset_street_betting_state(current_street)

            if current_street == "Preflop" and current_hand.button_seat_num is not None and not positions_assigned_for_hand:
                valid_player_seat_info = {s:i for s,i in current_hand.player_seat_info.items() if i['name'] is not None and i.get('chips',0) > 0}
                if valid_player_seat_info:
                     current_hand.player_positions = assign_player_positions(valid_player_seat_info, current_hand.button_seat_num)
                     positions_assigned_for_hand = True
            
            if current_street == "Summary": # Pegar board cards aqui para o objeto temporário
                # Procurar "Board [...]" em todo o bloco, pois pode não estar na linha de summary
                m_board_search = RE_BOARD_CARDS.search(hand_text_block)
                if m_board_search:
                    current_hand.board_cards = m_board_search.group(1).split(' ')
            continue # Pula para a próxima linha após detectar nova street

        m = RE_TABLE_INFO.match(line)
        if m:
            current_hand.table_id = m.group(1) # O ID da mesa pode ser composto, ex '123456789 10'
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
        
        # Lógica para atribuir posições se ainda não foram e estamos em uma street de ação
        if not positions_assigned_for_hand and current_hand.button_seat_num is not None and \
           current_street not in ["Summary", "Pre-deal", "Showdown"] and \
           any((info.get('name') or "") + ": " in line for seat, info in current_hand.player_seat_info.items()): # Verifica se é uma linha de ação
            valid_player_seat_info = {s:i for s,i in current_hand.player_seat_info.items() if i['name'] is not None and i.get('chips',0) > 0}
            if valid_player_seat_info:
                 current_hand.player_positions = assign_player_positions(valid_player_seat_info, current_hand.button_seat_num)
                 positions_assigned_for_hand = True
        
        # Parsing de Ações
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
            if player_name_from_action: current_hand.set_hole_cards(player_name_from_action, cards_shown) # Atualiza hole_cards se mostrado
            action_parsed = True
        elif RE_DOESNT_SHOW.match(line): m = RE_DOESNT_SHOW.match(line); player_name_from_action = m.group(1); action_data.update({'action': 'doesnt_show_hand'}); action_parsed = True
        elif RE_MUCKS_HAND.match(line): m = RE_MUCKS_HAND.match(line); player_name_from_action = m.group(1); action_data.update({'action': 'mucks_hand'}); action_parsed = True
        
        # Board cards podem aparecer em linhas separadas como "Board [cards]"
        m_board = RE_BOARD_CARDS.match(line)
        if m_board:
            current_hand.board_cards = m_board.group(1).split(' ')
            # Não é uma ação de jogador, então não precisa de action_data populado, mas atualiza o estado da mão.
            # Você pode querer adicionar uma "ação" do tipo 'board_dealt' se isso for útil para o DB.
            # Por agora, apenas atualiza o objeto current_hand.
            continue # Não processa mais esta linha como uma ação de jogador

        if action_parsed and player_name_from_action:
            action_data['player'] = player_name_from_action
            pos = current_hand.player_positions.get(player_name_from_action, "N/A_NoPosYet")
            action_data['position'] = pos
            if current_hand.hero_name and player_name_from_action == current_hand.hero_name:
                action_data['hero'] = True
            
            # Adiciona a ação ao objeto current_hand para que ele possa calcular o estado do pote, etc.
            # Apenas adiciona ações de jogo real (não summary/showdown aqui, são tratadas separadamente se necessário)
            if action_data['action'] and action_data['player'] and current_street not in ["Summary", None, "Pre-deal", "Showdown"]:
                 current_hand.add_action(action_data)
            elif action_data['action'] in ['posts_ante', 'posts_sb', 'posts_bb'] and current_street == "Pre-deal":
                current_hand.add_action(action_data) # Blinds e antes são adicionados
            elif action_data['action'] in ['shows_hand', 'mucks_hand', 'doesnt_show_hand', 'collected_pot', 'uncalled_bet_returned'] and current_street in ["Showdown", "Summary"]:
                 current_hand.actions.append(action_data) # Adiciona ações de final de mão à lista também

    # Finalizar agressor da última street de ação
    if current_street == "River": current_hand.river_aggressor = current_hand.current_street_aggressor
    elif current_street == "Turn" and not current_hand.streets_seen.intersection({"River", "Showdown", "Summary"}):
        current_hand.turn_aggressor = current_hand.current_street_aggressor
    elif current_street == "Flop" and not current_hand.streets_seen.intersection({"Turn", "River", "Showdown", "Summary"}):
        current_hand.flop_aggressor = current_hand.current_street_aggressor
    # PFA (Preflop Aggressor) é definido como o último raiser/better no pré-flop durante add_action

    if current_hand:
        current_hand.determine_actors_order() # Para ter a ordem de ação nas streets
    return current_hand # Retorna o objeto PokerHand populado


def parse_poker_log_file_to_hands_and_save_to_db(log_content, conn):
    hand_texts = []
    current_block_for_split = []
    for line_in_block in log_content.strip().split('\n'):
        if line_in_block.startswith("PokerStars Hand #") and current_block_for_split:
            hand_texts.append("\n".join(current_block_for_split))
            current_block_for_split = [line_in_block]
        elif line_in_block.strip() or current_block_for_split: # Adiciona linhas em branco se estiverem dentro de um bloco
            current_block_for_split.append(line_in_block)
    if current_block_for_split:
        hand_texts.append("\n".join(current_block_for_split))

    parsed_hand_objects_for_stats = [] # Para o cálculo de stats existente, temporariamente
    processed_hand_ids_in_db_this_run = set()
    newly_inserted_count = 0

    if not hand_texts: return parsed_hand_objects_for_stats, newly_inserted_count

    cursor = conn.cursor()
    for i, hand_text_block in enumerate(hand_texts):
        # Verificação rápida de ID
        temp_hand_id_match = RE_HAND_HEADER.match(hand_text_block.split('\n')[0])
        current_hand_history_id = None
        if temp_hand_id_match:
            current_hand_history_id = temp_hand_id_match.group(1)
            cursor.execute("SELECT hand_db_id FROM hands WHERE hand_history_id = ?", (current_hand_history_id,))
            if cursor.fetchone():
                if current_hand_history_id not in processed_hand_ids_in_db_this_run:
                    # print(f"Mão {current_hand_history_id} já no DB (verificação rápida). Pulando.")
                    processed_hand_ids_in_db_this_run.add(current_hand_history_id)
                continue # Pula para a próxima mão

        hand_obj = parse_hand_history_to_object(hand_text_block) # Parseia para o objeto Python
        if hand_obj and hand_obj.hand_id:
            # Tenta salvar no DB. O método save_to_db lida com duplicatas internamente também.
            hand_db_id = hand_obj.save_to_db(conn)
            if hand_db_id: # Se foi inserida com sucesso (não era duplicata)
                newly_inserted_count += 1
                processed_hand_ids_in_db_this_run.add(hand_obj.hand_id)
                parsed_hand_objects_for_stats.append(hand_obj) # Adiciona para cálculo de stats temporário
            elif current_hand_history_id and current_hand_history_id not in processed_hand_ids_in_db_this_run:
                # print(f"Mão {current_hand_history_id} já existia no DB (detectado em save_to_db).")
                processed_hand_ids_in_db_this_run.add(current_hand_history_id)
                # Ainda adiciona ao parsed_hand_objects_for_stats se você quiser que stats sejam calculadas
                # mesmo para mãos já no DB (útil se a lógica de stats mudou mas o parsing não)
                # Para uma inserção "limpa", você não adicionaria aqui.
                # Mas para manter o cálculo de stats existente funcionando por ora:
                parsed_hand_objects_for_stats.append(hand_obj)


        if (i + 1) % 100 == 0: # Commit a cada 100 mãos
            conn.commit()
            print(f"Processadas {i+1}/{len(hand_texts)} mãos. {newly_inserted_count} novas inseridas no DB.")

    conn.commit() # Commit final para quaisquer mãos restantes
    print(f"Total de {newly_inserted_count} novas mãos inseridas no DB nesta execução.")
    return parsed_hand_objects_for_stats, newly_inserted_count


# Mapeamentos e Factories (sem alterações, pois ainda são usados por PlayerStats)
POSITION_CATEGORIES = {
    "UTG": "EP", "UTG+1": "EP", "UTG+2": "EP",
    "MP": "MP", "MP1": "MP", "LJ": "MP", "HJ": "MP",
    "CO": "CO", "BTN": "BTN", "SB": "SB"
}
PF_POS_CATS_FOR_STATS = ["EP", "MP", "CO", "BTN", "SB"]
PF_POS_CATS_FOR_CALL_STATS = ["EP", "MP", "CO", "BTN", "SB", "BB"]

def _get_simplified_hand_category_from_description(description_str):
    if not description_str: return "desconhecido"
    desc_lower = description_str.lower()
    topo_keywords = ["straight flush", "four of a kind", "quads", "full house", "flush", "straight", "three of a kind", "two pair"]
    pair_keywords = ["a pair", "one pair"] # "pair" deve vir depois de "two pair"
    high_card_keywords = ["high card"]

    for kw in topo_keywords:
        if kw in desc_lower: return "topo"
    # Se chegou aqui, não é "topo"
    for kw in pair_keywords: # Verifica "one pair"
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
    if not th or pct is None: return None # Adicionado pct is None
    under_max, gto_max = th
    if pct <= under_max: return "under"
    elif pct <= gto_max: return "gto"
    return "over"

class PlayerStats:
    # ... (A CLASSE PlayerStats INTEIRA PERMANECE IGUAL POR ENQUANTO) ...
    # ... (COPIE E COLE A SUA CLASSE PlayerStats ORIGINAL AQUI) ...
    # ... (INCLUINDO __init__, todos os getters de display, to_dict_display, etc.) ...
    # A única modificação necessária na PlayerStats para funcionar com a nova abordagem de tamanho de pote
    # seria garantir que `get_bet_size_group` lide com None
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
            return "N/A" # Lida com None
        if bet_percentage_pot <= 29.99: return "0-29%"
        if bet_percentage_pot <= 45.99: return "30-45%"
        if bet_percentage_pot <= 56.99: return "46-56%"
        if bet_percentage_pot <= 70.99: return "57-70%"
        # Ajustei para cobrir o gap que existia e alinhar com SIZE_MAP do helper.py
        # Se SIZE_MAP for a fonte da verdade, use os limites dela
        if bet_percentage_pot <= 100.99: return "80-100%" # Antes era 71-100%
        return "101%+"

    def get_stat_percentage(self, actions, opportunities):
        if opportunities == 0: return 0.0
        return (actions / opportunities) * 100

    def get_raw_stat_value(self, stat_name_key):
        # Esta função precisa ser mantida/adaptada se PlayerStats for populado por SQL
        # ou se a geração de HTML for buscar direto do SQL com lógica similar
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
        # FTS Street Size (%) -> FTS Flop 0-29% (%)
        if parts[0] == "FTS" and len(parts) == 4 and parts[3] == "(%)":
            street, size_group_key = parts[1], parts[2]
            actions = self.fold_to_bet_actions_by_size.get(street, {}).get(size_group_key, 0)
            opportunities = self.fold_to_bet_opportunities_by_size.get(street, {}).get(size_group_key, 0)
            return self.get_stat_percentage(actions, opportunities)

        # Fold CBet Flop IP/OOP Size (%) -> Fold CBet Flop IP 0-29% (%)
        if parts[0] == "Fold" and parts[1] == "CBet" and parts[2] == "Flop" and parts[3] in ["IP", "OOP"] and len(parts) == 5 and parts[4].endswith("(%)"):
            size_group_key = parts[4].replace(" (%)","") # Correção aqui, o size group é parts[3] e parts[4] é "(%)"
            ip_oop_indicator = parts[3]
            # Precisa reestruturar a chave: "Fold CBet Flop IP 0-29% (%)"
            # A chave correta seria "Fold CBet Flop IP <size_group> (%)"
            # Então, parts[4] deveria ser o size_group e parts[5] seria "(%)"
            # Ajustando a lógica de split ou a chave
            # Se a chave é "Fold CBet Flop IP 0-29% (%)", parts são:
            # 0: Fold, 1: CBet, 2: Flop, 3: IP, 4: 0-29%, 5: (%)
            if len(parts) == 6 and parts[5] == "(%)":
                ip = ip_oop_indicator == "IP"
                size_group_key = parts[4]
                if ip:
                    actions = self.fold_to_flop_cbet_ip_actions_by_size.get(size_group_key, 0)
                    opportunities = self.fold_to_flop_cbet_ip_opportunities_by_size.get(size_group_key, 0)
                else: # OOP
                    actions = self.fold_to_flop_cbet_oop_actions_by_size.get(size_group_key, 0)
                    opportunities = self.fold_to_flop_cbet_oop_opportunities_by_size.get(size_group_key, 0)
                return self.get_stat_percentage(actions, opportunities)


        # Fold Donk Street Size (%) -> Fold Donk Flop 0-29% (%)
        if parts[0] == "Fold" and parts[1] == "Donk" and parts[2] in ["Flop", "Turn", "River"] and len(parts) == 5 and parts[4] == "(%)":
            street = parts[2]
            size_group_key = parts[3]
            actions = getattr(self, f"fold_to_donk_bet_{street.lower()}_actions_by_size").get(size_group_key, 0)
            opportunities = getattr(self, f"fold_to_donk_bet_{street.lower()}_opportunities_by_size").get(size_group_key, 0)
            return self.get_stat_percentage(actions, opportunities)

        # FTS River LineType Size (%) -> FTS River BBB 0-29% (%)
        if parts[0] == "FTS" and len(parts) == 5 and parts[1] == "River" and parts[4] == "(%)":
            line_type, size_group_key = parts[2], parts[3]
            actions = self.fold_to_river_bet_by_line_actions_by_size.get(line_type, {}).get(size_group_key, 0)
            opportunities = self.fold_to_river_bet_by_line_opportunities_by_size.get(line_type, {}).get(size_group_key, 0)
            return self.get_stat_percentage(actions, opportunities)

        # River LineType Size HandCat (%) -> River BBB 0-29% Air (%)
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
            except Exception: pass # Evitar quebrar se o formato da chave for inesperado
        return 0.0

    # --- Propriedades de Display (SEM ALTERAÇÕES POR ENQUANTO) ---
    # COPIE E COLE TODAS AS SUAS PROPRIEDADES DE DISPLAY AQUI
    # Ex: @property def vpip_percentage_display(self): ...
    #     @property def pfr_percentage_display(self): ...
    #     etc.
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
        # Usar FOLD_CLASS_THRESHOLDS para C/F também, pois é um tipo de fold
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
        if hand_category_key == 'air': # Adiciona classificação apenas para 'air' (blefes)
            label = _classify_percentage(size_group, percentage, BLUFF_CLASS_THRESHOLDS)
            if label:
                result += f" {label.capitalize()}"
        return result

    def _get_river_bluff_value_counts(self, line_type, size_group):
        line_data = self.river_bet_called_composition_by_line.get(line_type, {})
        size_data = line_data.get(size_group, {})
        total = size_data.get('total_showdowns', 0)
        bluff = size_data.get('air', 0) # 'air' é considerado bluff
        return bluff, total

    def get_river_bluff_percentage_display(self, line_type, size_group):
        bluff, total = self._get_river_bluff_value_counts(line_type, size_group)
        return f"{self.get_stat_percentage(bluff, total):.1f}% ({bluff}/{total})"

    def get_river_value_percentage_display(self, line_type, size_group):
        bluff, total = self._get_river_bluff_value_counts(line_type, size_group)
        value = total - bluff # Value é o resto
        return f"{self.get_stat_percentage(value, total):.1f}% ({value}/{total})"

    def get_river_bluff_over_under_display(self, line_type, size_group):
        bluff, total = self._get_river_bluff_value_counts(line_type, size_group)
        if total == 0: return "N/A"
        pct = self.get_stat_percentage(bluff, total)
        label_key = _classify_percentage(size_group, pct, BLUFF_CLASS_THRESHOLDS)
        if not label_key: return f"{pct:.1f}%"
        # Mapeamento mais descritivo para o display
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
        
        # Helper.py SIZE_KEYS para consistência ['0-29%', '30-45%', '46-56%', '57-70%', '101%+']
        # O seu PlayerStats usa "80-100%" que não está em helper.py's SIZE_MAP
        # Ajustar size_groups_for_dict para usar as chaves de SIZE_MAP do helper.py se for o caso
        # ou adicionar "80-100%" ao helper.py. Por ora, usarei os que estão no seu PlayerStats.
        size_groups_for_dict = ["0-29%", "30-45%", "46-56%", "57-70%", "80-100%", "101%+", "N/A"]
        line_types_for_dict = ["BBB", "BXB", "XBB", "XXB"] # Linhas de River
        
        for street in ["Flop", "Turn", "River"]:
            for sg in size_groups_for_dict:
                if sg == "N/A" : continue
                key = f"FTS {street} {sg} (%)" # A chave para get_raw_stat_value precisa ser precisa
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
            key = f"CF Turn {sg} (%)" # Call-Fold Turn
            d[key] = self.get_call_fold_turn_display(sg)
        
        hand_categories_display_map_for_dict = {"topo": "Topo", "bluff_catcher": "BluffCatcher", "air": "Air"}
        for lt in line_types_for_dict:
            for sg in size_groups_for_dict:
                if sg == "N/A": continue
                # Verifica se há dados para esta linha/tamanho antes de adicionar ao dicionário
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


def player_stats_factory():
    return PlayerStats(None) # Player name será definido depois

def calculate_player_stats(hands_objects_list): # Esta função ainda opera em memória
    player_stats_data = defaultdict(player_stats_factory)

    for hand_idx, hand in enumerate(hands_objects_list): # Recebe a lista de PokerHand objects
        if not hand.player_positions: continue

        dealt_players = set()
        for seat_num, seat_info in hand.player_seat_info.items():
            if seat_info['name'] and seat_info.get('chips', 0) > 0 :
                # Adicionar apenas se o jogador tem posição (participou da mão ativamente)
                # Ou se tem hole cards (é o hero)
                if seat_info['name'] in hand.player_positions or seat_info['name'] == hand.hero_name: # Correção aqui
                    dealt_players.add(seat_info['name'])
        
        if not dealt_players and hand.player_positions: # Fallback se dealt_players estiver vazio mas há posições
            dealt_players = set(hand.player_positions.keys())


        for player_name in dealt_players:
            if not player_name: continue # Pula se o nome for None/vazio
            if player_stats_data[player_name].player_name is None:
                player_stats_data[player_name].player_name = player_name
            player_stats_data[player_name].hands_played += 1
        
        # --- O RESTO DA LÓGICA DE calculate_player_stats PERMANECE IGUAL ---
        # --- COPIE E COLE O RESTANTE DA SUA FUNÇÃO calculate_player_stats ORIGINAL AQUI ---
        # --- ELA IRÁ OPERAR SOBRE A `hands_objects_list` ---
        preflop_actions = [a for a in hand.actions if a.get('street') == 'Preflop']
        flop_actions = [a for a in hand.actions if a.get('street') == 'Flop']
        turn_actions = [a for a in hand.actions if a.get('street') == 'Turn']
        river_actions = [a for a in hand.actions if a.get('street') == 'River']
        showdown_actions = [a for a in hand.actions if a.get('street') == 'Showdown' or (a.get('action') == 'shows_hand' and a.get('street') == 'Summary')]

        # --- Lógica Pré-Flop (VPIP, PFR, 3Bet, etc.) ---
        for player_name in dealt_players:
            if not player_name: continue
            player_pf_actions_no_blinds = [a for a in preflop_actions if a.get('player') == player_name and a.get('action') not in ['posts_ante', 'posts_sb', 'posts_bb']]
            player_stats_data[player_name].vpip_opportunities += 1
            player_stats_data[player_name].pfr_opportunities += 1 # Oportunidade de PFR é a mesma de VPIP (qualquer mão que não seja walk)
            made_vpip_action = any(a.get('action') in ['calls', 'bets', 'raises'] for a in player_pf_actions_no_blinds)
            made_pfr_action = any(a.get('action') in ['bets', 'raises'] for a in player_pf_actions_no_blinds) # PFR = bet ou raise
            if made_vpip_action: player_stats_data[player_name].vpip_actions += 1
            if made_pfr_action: player_stats_data[player_name].pfr_actions += 1

        current_bet_level_pf = 0 # 0=blinds, 1=openraise, 2=3bet, 3=4bet
        last_raiser_pf = None
        open_raise_made_pf = False
        player_who_open_raised_pf = None
        player_who_3bet_pf = None # Quem fez o 3bet
        callers_after_open_raise_pf = 0
        limpers_before_first_raise_pf = 0

        first_raise_action_index_pf = -1
        for idx, action in enumerate(preflop_actions):
            if action.get('action') in ['bets', 'raises']:
                first_raise_action_index_pf = idx
                break
        
        if first_raise_action_index_pf != -1: # Se houve raise
            for idx in range(first_raise_action_index_pf): # Ações antes do primeiro raise
                action = preflop_actions[idx]
                # Considera call como VPIP, exceto se for BB completando SB em pote não raisado
                is_bb_completing_unraised_pot = (
                    action.get('action') == 'calls' and
                    hand.get_player_position(action.get('player')) == 'BB' and
                    action.get('amount_to_call_for_player',0) == 0 and # Nada a mais para pagar
                    action.get('amount',0) + hand.bets_this_street_by_player.get(action.get('player'),0) <= hand.big_blind_amount
                )
                if action.get('action') == 'calls' and not is_bb_completing_unraised_pot and action.get('player') in dealt_players:
                    limpers_before_first_raise_pf += 1
        else: # Sem raises pré-flop (pote com limpers ou apenas blinds)
            for action in preflop_actions: # Conta todos os callers como limpers
                is_bb_completing_unraised_pot = (
                    action.get('action') == 'calls' and
                    hand.get_player_position(action.get('player')) == 'BB' and
                    action.get('amount_to_call_for_player',0) == 0 and 
                    action.get('amount',0) + hand.bets_this_street_by_player.get(action.get('player'),0) <= hand.big_blind_amount
                )
                if action.get('action') == 'calls' and not is_bb_completing_unraised_pot and action.get('player') in dealt_players:
                    limpers_before_first_raise_pf += 1
        
        for i, action in enumerate(preflop_actions):
            player = action.get('player')
            act = action.get('action')
            if not player or player not in dealt_players : continue # Ignora se não for um jogador da mão

            current_pos_raw = hand.get_player_position(player)
            current_pos_cat = POSITION_CATEGORIES.get(current_pos_raw) # EP, MP, CO, BTN, SB

            # Oportunidade de Open Raise
            if not open_raise_made_pf and current_pos_cat in PF_POS_CATS_FOR_STATS: # BB não pode open raise
                # Verifica se ninguém antes dele fez bet/raise (excluindo blinds)
                can_or = not any(prev_act.get('action') in ['bets', 'raises'] for prev_act in preflop_actions[:i])
                if can_or:
                    getattr(player_stats_data[player], f"open_raise_{current_pos_cat.lower()}_opportunities", 0) # Garante que existe
                    setattr(player_stats_data[player], f"open_raise_{current_pos_cat.lower()}_opportunities", getattr(player_stats_data[player], f"open_raise_{current_pos_cat.lower()}_opportunities", 0) + 1)

            # Oportunidade de Call Open Raise
            if open_raise_made_pf and current_bet_level_pf == 1 and player != player_who_open_raised_pf and current_pos_cat in PF_POS_CATS_FOR_CALL_STATS:
                # (BB está em PF_POS_CATS_FOR_CALL_STATS)
                getattr(player_stats_data[player], f"call_open_raise_{current_pos_cat.lower()}_opportunities", 0)
                setattr(player_stats_data[player], f"call_open_raise_{current_pos_cat.lower()}_opportunities", getattr(player_stats_data[player], f"call_open_raise_{current_pos_cat.lower()}_opportunities", 0) + 1)

            # Oportunidades de 3Bet, Squeeze, 4Bet
            if open_raise_made_pf and player != last_raiser_pf: # Se há um raiser anterior e não é o próprio jogador
                if current_bet_level_pf == 1: # Enfrentando um Open Raise (2bet) -> Oportunidade de 3Bet
                    player_stats_data[player].three_bet_pf_opportunities += 1
                    # Oportunidade de Squeeze: Havia limpers OU callers do open raise ANTES da ação do jogador atual
                    had_limpers_or_callers_before_3bet_opp = limpers_before_first_raise_pf > 0
                    # Verificar callers entre o open raise e a ação atual do jogador
                    or_action_idx_for_squeeze = -1
                    for idx_sq, a_obj_sq in enumerate(preflop_actions): # Encontrar ação do OR
                        if a_obj_sq.get('player') == player_who_open_raised_pf and a_obj_sq.get('action') in ['bets', 'raises']:
                            or_action_idx_for_squeeze = idx_sq
                            break
                    if or_action_idx_for_squeeze != -1:
                        for l_callers in range(or_action_idx_for_squeeze + 1, i): # Ações entre OR e jogador atual
                            if preflop_actions[l_callers].get('action') == 'calls' and preflop_actions[l_callers].get('player') != player_who_open_raised_pf:
                                had_limpers_or_callers_before_3bet_opp = True
                                break
                    if had_limpers_or_callers_before_3bet_opp:
                        player_stats_data[player].squeeze_pf_opportunities += 1

                elif current_bet_level_pf == 2: # Enfrentando um 3Bet -> Oportunidade de 4Bet
                    player_stats_data[player].four_bet_pf_opportunities += 1
            
            # Oportunidade de Fold BB vs Steal
            if current_pos_raw == 'BB' and open_raise_made_pf and current_bet_level_pf == 1 and last_raiser_pf and last_raiser_pf != player:
                raiser_pos_raw = hand.get_player_position(last_raiser_pf)
                if raiser_pos_raw == 'BTN': player_stats_data[player].fold_bb_vs_btn_steal_opportunities += 1
                elif raiser_pos_raw == 'CO': player_stats_data[player].fold_bb_vs_co_steal_opportunities += 1
                elif raiser_pos_raw == 'SB': player_stats_data[player].fold_bb_vs_sb_steal_opportunities += 1


            # Ações de Bet/Raise
            if act in ['bets', 'raises']:
                if not open_raise_made_pf: # Primeiro bet/raise da mão (Open Raise)
                    open_raise_made_pf = True
                    current_bet_level_pf = 1
                    last_raiser_pf = player
                    player_who_open_raised_pf = player
                    callers_after_open_raise_pf = 0 # Reseta callers após novo raise
                    if current_pos_cat in PF_POS_CATS_FOR_STATS: # BB não pode OR
                        setattr(player_stats_data[player], f"open_raise_{current_pos_cat.lower()}_actions", getattr(player_stats_data[player], f"open_raise_{current_pos_cat.lower()}_actions", 0) + 1)
                
                elif current_bet_level_pf == 1 and player != last_raiser_pf: # 3Bet
                    player_stats_data[player].three_bet_pf_actions += 1
                    if player_who_open_raised_pf: # OR original enfrenta 3bet
                        player_stats_data[player_who_open_raised_pf].fold_to_pf_3bet_opportunities += 1
                    
                    # Ação de Squeeze: verifica se a situação de squeeze (calculada na oportunidade) se concretizou
                    was_squeeze_situation_act = limpers_before_first_raise_pf > 0
                    or_action_index_act = -1 # Índice da ação do Open Raiser
                    for idx_act, a_obj_act in enumerate(preflop_actions):
                        if a_obj_act.get('player') == player_who_open_raised_pf and a_obj_act.get('action') in ['bets', 'raises']:
                            or_action_index_act = idx_act
                            break
                    if or_action_index_act != -1:
                        for k_idx_act in range(or_action_index_act + 1, i): # Ações entre OR e 3bettor
                            if preflop_actions[k_idx_act].get('action') == 'calls' and preflop_actions[k_idx_act].get('player') != player_who_open_raised_pf:
                                was_squeeze_situation_act = True; break
                    if was_squeeze_situation_act:
                        player_stats_data[player].squeeze_pf_actions +=1
                    
                    current_bet_level_pf = 2
                    player_who_3bet_pf = player # Guarda quem fez o 3bet
                    last_raiser_pf = player
                    callers_after_open_raise_pf = 0 # Reseta callers

                elif current_bet_level_pf == 2 and player != last_raiser_pf: # 4Bet
                    player_stats_data[player].four_bet_pf_actions += 1
                    if player_who_3bet_pf: # 3bettor original enfrenta 4bet
                        player_stats_data[player_who_3bet_pf].fold_to_pf_4bet_opportunities += 1
                    current_bet_level_pf = 3
                    last_raiser_pf = player
                    callers_after_open_raise_pf = 0 # Reseta callers
                # Lógica para 5bet+ pode ser adicionada aqui se necessário
            
            elif act == 'calls':
                if open_raise_made_pf and current_bet_level_pf == 1 and player != player_who_open_raised_pf:
                     # BB completando SB em pote não raisado não é call de OR
                    is_bb_completing_sb_only = (current_pos_raw == 'BB' and 
                                                player_who_open_raised_pf == hand.player_seat_info[hand.button_seat_num]['name'] and # SB foi o OR
                                                action.get('amount', 0) + hand.bets_this_street_by_player.get(player,0) - hand.big_blind_amount <= hand.big_blind_amount/2 # Chamando apenas a diferença
                                                ) # Aproximação
                    if not is_bb_completing_sb_only :
                        callers_after_open_raise_pf +=1
                        if current_pos_cat in PF_POS_CATS_FOR_CALL_STATS:
                            setattr(player_stats_data[player], f"call_open_raise_{current_pos_cat.lower()}_actions", getattr(player_stats_data[player], f"call_open_raise_{current_pos_cat.lower()}_actions", 0) + 1)
            
            elif act == 'folds':
                if current_bet_level_pf == 2 and player == player_who_open_raised_pf and last_raiser_pf != player: # OR folda para 3Bet
                    player_stats_data[player].fold_to_pf_3bet_actions += 1
                elif current_bet_level_pf == 3 and player == player_who_3bet_pf and last_raiser_pf != player: # 3Bettor folda para 4Bet
                    player_stats_data[player].fold_to_pf_4bet_actions += 1
                
                # Fold BB vs Steal (Ação)
                if current_pos_raw == 'BB' and open_raise_made_pf and current_bet_level_pf == 1 and last_raiser_pf and last_raiser_pf != player:
                    raiser_pos_raw = hand.get_player_position(last_raiser_pf)
                    if raiser_pos_raw == 'BTN': player_stats_data[player].fold_bb_vs_btn_steal_actions += 1
                    elif raiser_pos_raw == 'CO': player_stats_data[player].fold_bb_vs_co_steal_actions += 1
                    elif raiser_pos_raw == 'SB': player_stats_data[player].fold_bb_vs_sb_steal_actions += 1
        
        # --- Lógica Pós-Flop ---
        pfa = hand.preflop_aggressor # Nome do PFA

        for street_name, current_street_actions, actors_this_street, prev_street_aggressor_obj_attr, current_street_aggressor_obj_attr in [
            ("Flop", flop_actions, hand.flop_actors_in_order, "preflop_aggressor", "flop_aggressor"),
            ("Turn", turn_actions, hand.turn_actors_in_order, "flop_aggressor", "turn_aggressor"),
            ("River", river_actions, hand.river_actors_in_order, "turn_aggressor", "river_aggressor")
        ]:
            if not current_street_actions: continue # Pula a street se não houver ações

            aggressor_ps = getattr(hand, prev_street_aggressor_obj_attr, None) # Nome do agressor da street anterior

            # Encontrar a primeira ação do agressor da street anterior nesta street
            aggressor_ps_first_action_obj_this_street = None
            aggressor_ps_action_idx_this_street = -1 # Índice da ação do agressor_ps
            if aggressor_ps:
                for idx, act_obj in enumerate(current_street_actions):
                    if act_obj.get('player') == aggressor_ps:
                        aggressor_ps_first_action_obj_this_street = act_obj
                        aggressor_ps_action_idx_this_street = idx
                        break
            
            # Oportunidade de CBet para o agressor_ps
            can_aggressor_ps_cbet_this_street = False
            if aggressor_ps and aggressor_ps_first_action_obj_this_street :
                 # Ninguém betou/raisou ANTES do agressor_ps agir nesta street
                 can_aggressor_ps_cbet_this_street = not any(a['action'] in ['bets', 'raises'] for a in current_street_actions[:aggressor_ps_action_idx_this_street])

            if aggressor_ps and aggressor_ps_first_action_obj_this_street and can_aggressor_ps_cbet_this_street:
                cbet_opp = f"cbet_{street_name.lower()}_opportunities"
                cbet_act = f"cbet_{street_name.lower()}_actions"
                player_stats_data[aggressor_ps].__setattr__(cbet_opp, getattr(player_stats_data[aggressor_ps], cbet_opp, 0) + 1)

                if aggressor_ps_first_action_obj_this_street.get('action') == 'bets': # CBet feita
                    player_stats_data[aggressor_ps].__setattr__(cbet_act, getattr(player_stats_data[aggressor_ps], cbet_act, 0) + 1)
                    
                    # Oponentes enfrentam CBet
                    player_faced_cbet_this_instance = set() # Jogadores que já tiveram opp de fold to cbet nesta instância
                    for k_fcbet in range(aggressor_ps_action_idx_this_street + 1, len(current_street_actions)):
                        reaction = current_street_actions[k_fcbet]
                        reactor = reaction.get('player')
                        if not reactor or reactor == aggressor_ps or reactor not in dealt_players: continue
                        if reactor not in player_faced_cbet_this_instance: # Apenas primeira oportunidade por jogador por CBet
                            player_faced_cbet_this_instance.add(reactor)
                            ftcbet_opp = f"fold_to_{street_name.lower()}_cbet_opportunities"
                            ftcbet_act = f"fold_to_{street_name.lower()}_cbet_actions"
                            player_stats_data[reactor].__setattr__(ftcbet_opp, getattr(player_stats_data[reactor], ftcbet_opp, 0) + 1)
                            if reaction.get('action') == 'folds':
                                player_stats_data[reactor].__setattr__(ftcbet_act, getattr(player_stats_data[reactor], ftcbet_act, 0) + 1)
                            
                            # Fold to CBet por Posição (IP/OOP) e Size (Flop, Turn, River)
                            # Precisa do nome da street para a função is_player_ip_on_street
                            current_street_name_for_ip_check = aggressor_ps_first_action_obj_this_street.get('street')
                            is_reactor_ip = hand.is_player_ip_on_street(reactor, aggressor_ps, actors_this_street, current_street_name_for_ip_check)
                            
                            if is_reactor_ip is not None: # Se a posição pôde ser determinada
                                if street_name == "Flop":
                                    if is_reactor_ip: player_stats_data[reactor].fold_to_flop_cbet_ip_opportunities += 1
                                    else: player_stats_data[reactor].fold_to_flop_cbet_oop_opportunities += 1
                                    if reaction.get('action') == 'folds':
                                        if is_reactor_ip: player_stats_data[reactor].fold_to_flop_cbet_ip_actions += 1
                                        else: player_stats_data[reactor].fold_to_flop_cbet_oop_actions += 1
                                    
                                    # Fold to CBet Flop por Size e IP/OOP
                                    bet_faced_amount = reaction.get('bet_faced_by_player_amount',0) # CBet amount
                                    pot_when_cbet_made = reaction.get('pot_when_bet_was_made',0) # Pote antes da CBet
                                    if bet_faced_amount > 0 and pot_when_cbet_made > 0:
                                        pct_cbet = (bet_faced_amount / pot_when_cbet_made) * 100
                                        sg_cbet = player_stats_data[reactor].get_bet_size_group(pct_cbet)
                                        if is_reactor_ip:
                                            player_stats_data[reactor].fold_to_flop_cbet_ip_opportunities_by_size[sg_cbet] += 1
                                            if reaction.get('action') == 'folds':
                                                player_stats_data[reactor].fold_to_flop_cbet_ip_actions_by_size[sg_cbet] += 1
                                        else: # OOP
                                            player_stats_data[reactor].fold_to_flop_cbet_oop_opportunities_by_size[sg_cbet] += 1
                                            if reaction.get('action') == 'folds':
                                                player_stats_data[reactor].fold_to_flop_cbet_oop_actions_by_size[sg_cbet] += 1
                                elif street_name == "Turn":
                                    if is_reactor_ip: player_stats_data[reactor].fold_to_turn_cbet_ip_opportunities += 1
                                    else: player_stats_data[reactor].fold_to_turn_cbet_oop_opportunities += 1
                                    if reaction.get('action') == 'folds':
                                        if is_reactor_ip: player_stats_data[reactor].fold_to_turn_cbet_ip_actions += 1
                                        else: player_stats_data[reactor].fold_to_turn_cbet_oop_actions += 1
                                elif street_name == "River":
                                    if is_reactor_ip: player_stats_data[reactor].fold_to_river_cbet_ip_opportunities += 1
                                    else: player_stats_data[reactor].fold_to_river_cbet_oop_opportunities += 1
                                    if reaction.get('action') == 'folds':
                                        if is_reactor_ip: player_stats_data[reactor].fold_to_river_cbet_ip_actions += 1
                                        else: player_stats_data[reactor].fold_to_river_cbet_oop_actions += 1
                
                # CBet Flop IP/OOP (Ação do CBetter)
                if street_name == "Flop" and aggressor_ps == pfa: # Só PFA pode CBet Flop
                    current_street_name_for_ip_check_cbetter = aggressor_ps_first_action_obj_this_street.get('street')
                    is_pfa_ip_cbet = hand.is_player_ip_on_street(pfa, pfa, actors_this_street, current_street_name_for_ip_check_cbetter) # Compara PFA consigo mesmo
                    if is_pfa_ip_cbet is not None:
                        if is_pfa_ip_cbet: player_stats_data[pfa].cbet_flop_ip_opportunities += 1
                        else: player_stats_data[pfa].cbet_flop_oop_opportunities += 1
                        if aggressor_ps_first_action_obj_this_street.get('action') == 'bets': # CBet feita
                            if is_pfa_ip_cbet: player_stats_data[pfa].cbet_flop_ip_actions += 1
                            else: player_stats_data[pfa].cbet_flop_oop_actions += 1
            
            # Lógica para Donk, Probe, Bet vs Missed CBet, Check-Call/Fold/Raise
            player_checked_this_street = {} # player_name: index_of_check_action
            player_faced_bet_after_check_this_street = {} # player_name: True (se já enfrentou bet após check)
            
            # Situação de PFA ter dado check (oportunidade para outros betarem vs missed cbet)
            aggressor_ps_did_skip_cbet_opp_val = (
                aggressor_ps and aggressor_ps_first_action_obj_this_street and
                aggressor_ps_first_action_obj_this_street.get('action') == 'checks' and
                can_aggressor_ps_cbet_this_street # Garante que era uma oportunidade de CBet
            )

            donk_opp_counted_this_street = set()
            probe_opp_counted_this_street = set()
            bvmcb_opp_counted_this_street = set() # Bet Vs Missed CBET

            for i_action, action_obj in enumerate(current_street_actions):
                player = action_obj.get('player')
                act = action_obj.get('action')
                if not player or player not in dealt_players: continue

                # Oportunidade de Donk Bet: Jogador OOP ao agressor da street anterior, e agressor ainda não agiu.
                # E ninguém betou/raisou antes do jogador nesta street.
                no_prior_aggressive_action_this_street_for_opp = not any(
                    a.get('action') in ['bets', 'raises'] and current_street_actions.index(a) < i_action
                    for a in current_street_actions
                )
                if aggressor_ps and player != aggressor_ps and player not in donk_opp_counted_this_street:
                    aggressor_ps_already_acted_this_street = aggressor_ps_first_action_obj_this_street and aggressor_ps_action_idx_this_street < i_action
                    if not aggressor_ps_already_acted_this_street and no_prior_aggressive_action_this_street_for_opp:
                        # Precisa do nome da street para is_player_oop_to_another
                        current_street_name_for_donk_check = action_obj.get('street')
                        is_oop_to_aggressor_ps = hand.is_player_oop_to_another(player, aggressor_ps, actors_this_street)
                        if is_oop_to_aggressor_ps: # Só OOP pode Donk Bet
                            donk_opp_s = f"donk_bet_{street_name.lower()}_opportunities"
                            player_stats_data[player].__setattr__(donk_opp_s, getattr(player_stats_data[player], donk_opp_s, 0) + 1)
                            donk_opp_counted_this_street.add(player)
                
                # Oportunidade de Probe Bet: Não houve agressor na street anterior (ex: todos deram check no flop),
                # e jogador é o primeiro a ter chance de betar ou já houve checks antes dele.
                # E ninguém betou/raisou antes do jogador nesta street.
                final_aggressor_of_prev_s = getattr(hand, prev_street_aggressor_obj_attr, None)
                if final_aggressor_of_prev_s is None and street_name in ["Turn", "River"] and player not in probe_opp_counted_this_street:
                    if no_prior_aggressive_action_this_street_for_opp:
                        probe_opp_s = f"probe_bet_{street_name.lower()}_opportunities"
                        player_stats_data[player].__setattr__(probe_opp_s, getattr(player_stats_data[player], probe_opp_s, 0) + 1)
                        probe_opp_counted_this_street.add(player)

                # Oportunidade de Bet vs Missed CBet: Agressor da street anterior deu check (aggressor_ps_did_skip_cbet_opp_val),
                # e jogador atual age DEPOIS do check do agressor_ps.
                # E ninguém betou/raisou entre o check do agressor_ps e a ação do jogador atual.
                if aggressor_ps_did_skip_cbet_opp_val and player != aggressor_ps and player not in bvmcb_opp_counted_this_street:
                    if aggressor_ps_action_idx_this_street < i_action : # Jogador age depois do check do PFA/AggrPS
                        no_bet_between_pfa_check_and_player_action = not any(
                            a.get('action') in ['bets', 'raises'] for a in current_street_actions[aggressor_ps_action_idx_this_street + 1 : i_action]
                        )
                        if no_bet_between_pfa_check_and_player_action:
                            bvmcb_opp_s = f"bet_vs_missed_cbet_{street_name.lower()}_opportunities"
                            player_stats_data[player].__setattr__(bvmcb_opp_s, getattr(player_stats_data[player], bvmcb_opp_s,0) + 1)
                            bvmcb_opp_counted_this_street.add(player)

                # --- Ações ---
                if act == 'bets':
                    is_first_bet_on_street_by_anyone_at_this_point = not any(
                        a.get('action') in ['bets', 'raises'] and current_street_actions.index(a) < i_action for a in current_street_actions
                    )
                    # Donk Bet (Ação)
                    if aggressor_ps and player != aggressor_ps:
                        aggressor_ps_acted_before_this_bet = aggressor_ps_first_action_obj_this_street and aggressor_ps_action_idx_this_street < i_action
                        if not aggressor_ps_acted_before_this_bet and is_first_bet_on_street_by_anyone_at_this_point: # Donk
                            current_street_name_for_donk_action = action_obj.get('street')
                            is_oop_for_donk_act = hand.is_player_oop_to_another(player, aggressor_ps, actors_this_street)
                            if is_oop_for_donk_act:
                                player_stats_data[player].__setattr__(f"donk_bet_{street_name.lower()}_actions", getattr(player_stats_data[player], f"donk_bet_{street_name.lower()}_actions", 0) + 1)
                                # Oponente (agressor_ps) enfrenta Donk Bet
                                player_faced_donk_this_instance = set()
                                bet_amt_donk = action_obj.get('amount',0)
                                pot_before_donk = action_obj.get('pot_total_before_action',0) # Pote ANTES do donk bet
                                sg_donk = player_stats_data[player].get_bet_size_group((bet_amt_donk / pot_before_donk) * 100 if pot_before_donk > 0 else None)
                                
                                for k_fdonk in range(i_action + 1, len(current_street_actions)):
                                    reaction_donk = current_street_actions[k_fdonk]
                                    reactor_donk = reaction_donk.get('player')
                                    # Apenas a reação do agressor da street anterior ao Donk Bet
                                    if reactor_donk == aggressor_ps and reactor_donk not in player_faced_donk_this_instance :
                                        player_faced_donk_this_instance.add(reactor_donk)
                                        ftdonk_opp = f"fold_to_donk_bet_{street_name.lower()}_opportunities"
                                        ftdonk_act = f"fold_to_donk_bet_{street_name.lower()}_actions"
                                        player_stats_data[reactor_donk].__setattr__(ftdonk_opp, getattr(player_stats_data[reactor_donk], ftdonk_opp, 0) + 1)
                                        if reaction_donk.get('action') == 'folds':
                                            player_stats_data[reactor_donk].__setattr__(ftdonk_act, getattr(player_stats_data[reactor_donk], ftdonk_act, 0) + 1)
                                        
                                        # Fold to Donk Bet por Size
                                        fold_to_donk_opp_by_size_attr = f"fold_to_donk_bet_{street_name.lower()}_opportunities_by_size"
                                        fold_to_donk_act_by_size_attr = f"fold_to_donk_bet_{street_name.lower()}_actions_by_size"
                                        getattr(player_stats_data[reactor_donk], fold_to_donk_opp_by_size_attr)[sg_donk] += 1
                                        if reaction_donk.get('action') == 'folds':
                                            getattr(player_stats_data[reactor_donk], fold_to_donk_act_by_size_attr)[sg_donk] += 1
                                        break # Apenas a primeira reação do agressor ao donk

                    # Probe Bet (Ação)
                    if final_aggressor_of_prev_s is None and street_name in ["Turn", "River"]:
                        if is_first_bet_on_street_by_anyone_at_this_point: # Probe bet
                            player_stats_data[player].__setattr__(f"probe_bet_{street_name.lower()}_actions", getattr(player_stats_data[player], f"probe_bet_{street_name.lower()}_actions", 0) + 1)
                            # Oponentes enfrentam Probe Bet
                            player_faced_probe_this_instance = set()
                            for k_fprobe in range(i_action + 1, len(current_street_actions)):
                                reaction_probe = current_street_actions[k_fprobe]
                                reactor_probe = reaction_probe.get('player')
                                if not reactor_probe or reactor_probe == player or reactor_probe not in dealt_players: continue
                                if reactor_probe not in player_faced_probe_this_instance:
                                    player_faced_probe_this_instance.add(reactor_probe)
                                    ftprobe_opp = f"fold_to_probe_bet_{street_name.lower()}_opportunities"
                                    ftprobe_act = f"fold_to_probe_bet_{street_name.lower()}_actions"
                                    player_stats_data[reactor_probe].__setattr__(ftprobe_opp, getattr(player_stats_data[reactor_probe], ftprobe_opp, 0) + 1)
                                    if reaction_probe.get('action') == 'folds':
                                        player_stats_data[reactor_probe].__setattr__(ftprobe_act, getattr(player_stats_data[reactor_probe], ftprobe_act, 0) + 1)
                                    # Não quebra, pois múltiplos jogadores podem enfrentar o probe.
                    
                    # Bet vs Missed CBet (Ação)
                    if aggressor_ps_did_skip_cbet_opp_val and player != aggressor_ps:
                         if aggressor_ps_action_idx_this_street < i_action: # Jogador agiu depois do check do PFA/AggrPS
                            no_bet_between_pfa_check_and_player_for_bvmcb_act = not any(
                                a.get('action') in ['bets', 'raises'] for a in current_street_actions[aggressor_ps_action_idx_this_street + 1 : i_action]
                            )
                            if no_bet_between_pfa_check_and_player_for_bvmcb_act: # É um Bet vs Missed CBet
                                player_stats_data[player].__setattr__(f"bet_vs_missed_cbet_{street_name.lower()}_actions", getattr(player_stats_data[player], f"bet_vs_missed_cbet_{street_name.lower()}_actions",0) + 1)
                                # Agressor original (que deu check) enfrenta o Bet vs Missed CBet
                                if aggressor_ps:
                                    pfa_reaction_to_bvmcb_idx = -1
                                    for k_bvmcb_react in range(i_action + 1, len(current_street_actions)):
                                        if current_street_actions[k_bvmcb_react].get('player') == aggressor_ps:
                                            pfa_reaction_to_bvmcb_idx = k_bvmcb_react
                                            break
                                    if pfa_reaction_to_bvmcb_idx != -1: # PFA/AggrPS reagiu
                                        pfa_reaction_obj = current_street_actions[pfa_reaction_to_bvmcb_idx]
                                        ft_bvmcb_opp = f"fold_to_bet_vs_missed_cbet_{street_name.lower()}_opportunities"
                                        ft_bvmcb_act = f"fold_to_bet_vs_missed_cbet_{street_name.lower()}_actions"
                                        player_stats_data[aggressor_ps].__setattr__(ft_bvmcb_opp, getattr(player_stats_data[aggressor_ps], ft_bvmcb_opp, 0) + 1)
                                        if pfa_reaction_obj.get('action') == 'folds':
                                            player_stats_data[aggressor_ps].__setattr__(ft_bvmcb_act, getattr(player_stats_data[aggressor_ps], ft_bvmcb_act, 0) + 1)
                
                elif act == 'raises': # Check-Raise ou Bet-Raise
                    # Oportunidade de Fold to Check-Raise para o bettor original
                    original_bettor_this_street = None
                    original_bet_action_idx = -1 # Índice da ação do bet original que foi raisado
                    # Procurar o bet anterior que este raise está respondendo
                    for k_orig_bet_search in range(i_action -1, -1, -1):
                        prev_action_obj_for_xr = current_street_actions[k_orig_bet_search]
                        if prev_action_obj_for_xr.get('action') == 'bets' and prev_action_obj_for_xr.get('player') != player: # É um bet de outro jogador
                            # Verificar se o raiser (player atual) deu check ANTES deste bet original
                            raiser_checked_before_this_raise_opp = any(
                                chk_act.get('player') == player and chk_act.get('action') == 'checks' and current_street_actions.index(chk_act) < k_orig_bet_search
                                for chk_act in current_street_actions[:k_orig_bet_search] # Ações antes do bet original
                            )
                            if raiser_checked_before_this_raise_opp: # Sim, é um check-raise
                                original_bettor_this_street = prev_action_obj_for_xr.get('player')
                                original_bet_action_idx = k_orig_bet_search
                                break # Encontrou o bet original que foi check-raisado
                    
                    if original_bettor_this_street: # Se foi um check-raise
                        ftxr_opp = f"fold_to_check_raise_{street_name.lower()}_opportunities"
                        ftxr_act = f"fold_to_check_raise_{street_name.lower()}_actions"
                        player_stats_data[original_bettor_this_street].__setattr__(ftxr_opp, getattr(player_stats_data[original_bettor_this_street], ftxr_opp, 0) + 1)
                        # Verificar a reação do bettor original ao check-raise
                        bettor_reaction_after_xr = None
                        for k_xr_react in range(i_action + 1, len(current_street_actions)): # Ações após o check-raise
                            if current_street_actions[k_xr_react].get('player') == original_bettor_this_street:
                                bettor_reaction_after_xr = current_street_actions[k_xr_react]
                                break
                        if bettor_reaction_after_xr and bettor_reaction_after_xr.get('action') == 'folds':
                            player_stats_data[original_bettor_this_street].__setattr__(ftxr_act, getattr(player_stats_data[original_bettor_this_street], ftxr_act, 0) + 1)

                # Lógica para Check-Call, Check-Fold, Check-Raise (Oportunidade e Ação)
                if act == 'checks':
                    player_checked_this_street[player] = i_action # Guarda o índice do check
                elif player in player_checked_this_street and not player_faced_bet_after_check_this_street.get(player, False):
                    idx_of_player_check = player_checked_this_street[player]
                    # Verificar se houve um bet/raise de outro jogador ENTRE o check do 'player' e sua ação atual
                    bet_faced_after_player_check = any(
                        current_street_actions[k_bet_check].get('action') in ['bets', 'raises'] and
                        current_street_actions[k_bet_check].get('player') != player
                        for k_bet_check in range(idx_of_player_check + 1, i_action) # Ações entre o check e a ação atual
                    )
                    if bet_faced_after_player_check: # Jogador deu check e enfrentou um bet/raise
                        player_faced_bet_after_check_this_street[player] = True # Marca que já teve a oportunidade
                        xc_opp = f"check_call_{street_name.lower()}_opportunities"
                        xf_opp = f"check_fold_{street_name.lower()}_opportunities"
                        xr_opp = f"check_raise_{street_name.lower()}_opportunities"
                        # Incrementa oportunidades
                        player_stats_data[player].__setattr__(xc_opp, getattr(player_stats_data[player], xc_opp,0)+1)
                        player_stats_data[player].__setattr__(xf_opp, getattr(player_stats_data[player], xf_opp,0)+1)
                        player_stats_data[player].__setattr__(xr_opp, getattr(player_stats_data[player], xr_opp,0)+1)
                        # Incrementa ação correspondente
                        if act == 'calls': player_stats_data[player].__setattr__(f"check_call_{street_name.lower()}_actions", getattr(player_stats_data[player], f"check_call_{street_name.lower()}_actions",0)+1)
                        elif act == 'folds': player_stats_data[player].__setattr__(f"check_fold_{street_name.lower()}_actions", getattr(player_stats_data[player], f"check_fold_{street_name.lower()}_actions",0)+1)
                        elif act == 'raises': player_stats_data[player].__setattr__(f"check_raise_{street_name.lower()}_actions", getattr(player_stats_data[player], f"check_raise_{street_name.lower()}_actions",0)+1)
                
                # PFA Skip CBet Flop & Check-Call/Fold/Raise
                if street_name == "Flop" and player == pfa and aggressor_ps_did_skip_cbet_opp_val: # PFA deu check no flop quando podia CBet
                    # Verificar se PFA enfrentou um bet APÓS seu check
                    bet_pfa_is_facing_idx = -1 # Índice do bet que o PFA está enfrentando
                    if aggressor_ps_action_idx_this_street != -1 : # Índice do check do PFA
                        for k_pfa_react_chk in range(aggressor_ps_action_idx_this_street + 1, i_action): # Ações entre check do PFA e ação atual do PFA
                            if current_street_actions[k_pfa_react_chk].get('player') != pfa and current_street_actions[k_pfa_react_chk].get('action') in ['bets', 'raises']:
                                bet_pfa_is_facing_idx = k_pfa_react_chk
                                break
                    if bet_pfa_is_facing_idx != -1: # PFA deu check, alguém betou, e agora PFA está agindo
                        player_stats_data[pfa].pfa_skipped_cbet_then_check_call_flop_opportunities +=1
                        player_stats_data[pfa].pfa_skipped_cbet_then_check_fold_flop_opportunities +=1
                        player_stats_data[pfa].pfa_skipped_cbet_then_check_raise_flop_opportunities +=1
                        if act == 'calls': player_stats_data[pfa].pfa_skipped_cbet_then_check_call_flop_actions +=1
                        elif act == 'folds': player_stats_data[pfa].pfa_skipped_cbet_then_check_fold_flop_actions +=1
                        elif act == 'raises': player_stats_data[pfa].pfa_skipped_cbet_then_check_raise_flop_actions +=1

                # Oportunidade de Bet River (se chegou a vez do jogador e não há aposta para pagar)
                if street_name == "River":
                    if action_obj.get('amount_to_call_for_player', 0) == 0 and player in dealt_players: # Nada a pagar, pode betar
                        player_stats_data[player].bet_river_opportunities += 1
                    if act == 'bets' and player in dealt_players: # Betou no river
                        player_stats_data[player].bet_river_actions += 1

        # --- Fold por Grupos de Sizes (FTS Street Size) ---
        for street_name_fts, current_street_actions_fts in [("Flop", flop_actions), ("Turn", turn_actions), ("River", river_actions)]:
            if not current_street_actions_fts: continue
            player_faced_bet_in_street_for_fts = set() # Para contar apenas uma oportunidade por jogador por street
            for action_detail in current_street_actions_fts:
                player_fts = action_detail.get('player')
                if not player_fts or player_fts not in dealt_players: continue
                
                bet_faced_val = action_detail.get('bet_faced_by_player_amount', 0)
                pot_when_bet_faced_val = action_detail.get('pot_when_bet_was_made', 0)

                if bet_faced_val > 0 and pot_when_bet_faced_val > 0 and player_fts not in player_faced_bet_in_street_for_fts:
                    player_faced_bet_in_street_for_fts.add(player_fts) # Marcar que este jogador já teve opp FTS nesta street
                    bet_percentage_fts = (bet_faced_val / pot_when_bet_faced_val) * 100
                    size_group_fts = player_stats_data[player_fts].get_bet_size_group(bet_percentage_fts)
                    
                    player_stats_data[player_fts].fold_to_bet_opportunities_by_size[street_name_fts][size_group_fts] += 1
                    if action_detail.get('action') == 'folds':
                        player_stats_data[player_fts].fold_to_bet_actions_by_size[street_name_fts][size_group_fts] += 1
        
        # --- Call-Fold Turn (pagou flop e foldou turn vs bet) ---
        # Jogadores que deram call no flop
        flop_callers_for_cf_turn = set()
        last_flop_bet_or_raise_action = None
        for act_fl in reversed(flop_actions): # Encontrar o último bet/raise do flop
            if act_fl.get('action') in ['bets', 'raises']:
                last_flop_bet_or_raise_action = act_fl
                break
        if last_flop_bet_or_raise_action:
            idx_last_flop_aggressive_action = flop_actions.index(last_flop_bet_or_raise_action)
            for act_fl_call in flop_actions[idx_last_flop_aggressive_action + 1:]: # Ações após o último bet/raise
                if act_fl_call.get('action') == 'calls' and act_fl_call.get('player') in dealt_players:
                    flop_callers_for_cf_turn.add(act_fl_call.get('player'))
        
        player_faced_bet_in_turn_for_cf = set()
        if flop_callers_for_cf_turn and turn_actions:
            for act_tu_cf in turn_actions:
                pl_tu_cf = act_tu_cf.get('player')
                if pl_tu_cf in flop_callers_for_cf_turn and pl_tu_cf not in player_faced_bet_in_turn_for_cf:
                    bet_faced_tu = act_tu_cf.get('bet_faced_by_player_amount',0)
                    pot_before_tu_bet = act_tu_cf.get('pot_when_bet_was_made',0)
                    if bet_faced_tu > 0 and pot_before_tu_bet > 0: # Enfrentou um bet no turn
                        player_faced_bet_in_turn_for_cf.add(pl_tu_cf) # Marca que teve oportunidade
                        bet_pct_tu = (bet_faced_tu / pot_before_tu_bet) * 100
                        sg_tu_cf = player_stats_data[pl_tu_cf].get_bet_size_group(bet_pct_tu)
                        
                        player_stats_data[pl_tu_cf].call_fold_turn_opportunities_by_size[sg_tu_cf] += 1
                        if act_tu_cf.get('action') == 'folds':
                            player_stats_data[pl_tu_cf].call_fold_turn_actions_by_size[sg_tu_cf] += 1

        # --- Call-Call-Fold River (pagou flop, pagou turn, foldou river vs bet) ---
        turn_callers_after_flop_call = set() # Jogadores que deram call no flop E no turn
        if flop_callers_for_cf_turn and turn_actions: # Reusa flop_callers
            last_turn_bet_or_raise_action = None
            for act_tu in reversed(turn_actions):
                if act_tu.get('action') in ['bets', 'raises']:
                    last_turn_bet_or_raise_action = act_tu
                    break
            if last_turn_bet_or_raise_action:
                idx_last_turn_aggressive_action = turn_actions.index(last_turn_bet_or_raise_action)
                for act_tu_call in turn_actions[idx_last_turn_aggressive_action + 1:]:
                    pl_tu_c = act_tu_call.get('player')
                    if act_tu_call.get('action') == 'calls' and pl_tu_c in flop_callers_for_cf_turn:
                        turn_callers_after_flop_call.add(pl_tu_c)
        
        player_faced_bet_in_river_for_ccf = set()
        if turn_callers_after_flop_call and river_actions:
            for act_ri_ccf in river_actions:
                pl_ri_ccf = act_ri_ccf.get('player')
                if pl_ri_ccf in turn_callers_after_flop_call and pl_ri_ccf not in player_faced_bet_in_river_for_ccf:
                    bet_faced_ri = act_ri_ccf.get('bet_faced_by_player_amount',0)
                    pot_before_ri_bet = act_ri_ccf.get('pot_when_bet_was_made',0)
                    if bet_faced_ri > 0 and pot_before_ri_bet > 0: # Enfrentou bet no river
                        player_faced_bet_in_river_for_ccf.add(pl_ri_ccf)
                        # Precisa do nome da street para a função is_player_ip_on_street
                        current_street_name_for_ccf_check = act_ri_ccf.get('street') # Deveria ser "River"
                        is_ip_ccf = hand.is_player_ip_on_street(pl_ri_ccf, pfa, hand.river_actors_in_order, current_street_name_for_ccf_check) # pfa pode não ser o agressor do river
                        # Melhor comparar com o agressor do river se houver, ou determinar IP de forma geral
                        river_aggressor_for_ccf = hand.river_aggressor if hand.river_aggressor else hand.turn_aggressor # Fallback
                        is_ip_ccf_better = hand.is_player_ip_on_street(pl_ri_ccf, river_aggressor_for_ccf, hand.river_actors_in_order, current_street_name_for_ccf_check)

                        attr_opp = "call_call_fold_river_ip_opportunities" if is_ip_ccf_better else "call_call_fold_river_oop_opportunities"
                        attr_act = "call_call_fold_river_ip_actions" if is_ip_ccf_better else "call_call_fold_river_oop_actions"
                        
                        player_stats_data[pl_ri_ccf].__setattr__(attr_opp, getattr(player_stats_data[pl_ri_ccf], attr_opp, 0) + 1)
                        if act_ri_ccf.get('action') == 'folds':
                            player_stats_data[pl_ri_ccf].__setattr__(attr_act, getattr(player_stats_data[pl_ri_ccf], attr_act, 0) + 1)
        
        # --- C/C/F vs Triple Barrel (Check/Call Flop, Check/Call Turn, Fold River vs 3rd Barrel) ---
        triple_bettor = None
        # PFA precisa ter betado todas as streets
        if pfa:
            pfa_bet_flop = any(a.get('player') == pfa and a.get('action') == 'bets' for a in flop_actions if a.get('street') == 'Flop')
            pfa_bet_turn = any(a.get('player') == pfa and a.get('action') == 'bets' for a in turn_actions if a.get('street') == 'Turn')
            pfa_bet_river = any(a.get('player') == pfa and a.get('action') == 'bets' for a in river_actions if a.get('street') == 'River')
            if pfa_bet_flop and pfa_bet_turn and pfa_bet_river:
                triple_bettor = pfa
        
        if triple_bettor:
            # Encontrar jogadores que deram check-call no flop vs o triple_bettor
            cc_flop_vs_tb = set()
            pfa_flop_bet_action = next((a for a in flop_actions if a.get('player') == triple_bettor and a.get('action') == 'bets'), None)
            if pfa_flop_bet_action:
                idx_pfa_flop_bet = flop_actions.index(pfa_flop_bet_action)
                for pl_ccf_f in dealt_players:
                    if pl_ccf_f == triple_bettor: continue
                    # Jogador deu check ANTES do bet do PFA e call DEPOIS
                    checked_before_pfa_bet_f = any(a_f_chk.get('player') == pl_ccf_f and a_f_chk.get('action') == 'checks' and flop_actions.index(a_f_chk) < idx_pfa_flop_bet for a_f_chk in flop_actions)
                    called_pfa_bet_f = any(a_f_call.get('player') == pl_ccf_f and a_f_call.get('action') == 'calls' and flop_actions.index(a_f_call) > idx_pfa_flop_bet for a_f_call in flop_actions)
                    if checked_before_pfa_bet_f and called_pfa_bet_f:
                        cc_flop_vs_tb.add(pl_ccf_f)
            
            # Desses, encontrar quem deu check-call no turn vs o triple_bettor
            cc_turn_vs_tb = set()
            pfa_turn_bet_action = next((a for a in turn_actions if a.get('player') == triple_bettor and a.get('action') == 'bets'), None)
            if pfa_turn_bet_action and cc_flop_vs_tb:
                idx_pfa_turn_bet = turn_actions.index(pfa_turn_bet_action)
                for pl_ccf_t in cc_flop_vs_tb:
                    checked_before_pfa_bet_t = any(a_t_chk.get('player') == pl_ccf_t and a_t_chk.get('action') == 'checks' and turn_actions.index(a_t_chk) < idx_pfa_turn_bet for a_t_chk in turn_actions)
                    called_pfa_bet_t = any(a_t_call.get('player') == pl_ccf_t and a_t_call.get('action') == 'calls' and turn_actions.index(a_t_call) > idx_pfa_turn_bet for a_t_call in turn_actions)
                    if checked_before_pfa_bet_t and called_pfa_bet_t:
                        cc_turn_vs_tb.add(pl_ccf_t)

            # Desses, ver quem foldou no river para o bet do triple_bettor
            pfa_river_bet_action = next((a for a in river_actions if a.get('player') == triple_bettor and a.get('action') == 'bets'), None)
            if pfa_river_bet_action and cc_turn_vs_tb:
                idx_pfa_river_bet = river_actions.index(pfa_river_bet_action)
                for pl_ccf_r_candidate in cc_turn_vs_tb:
                    # Jogador precisa ter chance de foldar para o 3rd barrel
                    # E sua ação de fold deve ser DEPOIS do bet do PFA no river
                    player_river_actions = [ra for ra in river_actions if ra.get('player') == pl_ccf_r_candidate and river_actions.index(ra) > idx_pfa_river_bet]
                    if player_river_actions: # Se o jogador agiu após o 3rd barrel
                        first_reaction_to_3rd_barrel = player_river_actions[0]
                        # Oportunidade de CCF vs Triple Barrel
                        player_stats_data[pl_ccf_r_candidate].ccf_triple_barrel_opportunities += 1
                        if first_reaction_to_3rd_barrel.get('action') == 'folds':
                            player_stats_data[pl_ccf_r_candidate].ccf_triple_barrel_actions += 1
        
        # --- B/B/F River vs Donk River (Jogador betou flop, betou turn, e foldou para um Donk Bet no River) ---
        for pl_bbf in dealt_players:
            if pl_bbf == pfa : continue # PFA não pode "donkar" a si mesmo
            
            player_bet_flop = any(a.get('player') == pl_bbf and a.get('action') == 'bets' for a in flop_actions)
            player_bet_turn = any(a.get('player') == pl_bbf and a.get('action') == 'bets' for a in turn_actions)

            if player_bet_flop and player_bet_turn: # Jogador foi o agressor no flop e turn
                # Verificar se houve um Donk Bet no River por outro jogador
                river_donk_bet_action = None
                river_donker = None
                for i_ra_donk, ra_donk in enumerate(river_actions):
                    # Donk é o primeiro bet da street, por um jogador que NÃO foi o agressor da street anterior (pl_bbf)
                    # E pl_bbf ainda não agiu no river
                    if ra_donk.get('action') == 'bets' and ra_donk.get('player') != pl_bbf:
                        # Verificar se pl_bbf ainda não agiu antes deste bet
                        pl_bbf_acted_before_this_river_bet = any(
                            a_river_pl.get('player') == pl_bbf and river_actions.index(a_river_pl) < i_ra_donk
                            for a_river_pl in river_actions
                        )
                        if not pl_bbf_acted_before_this_river_bet:
                            river_donk_bet_action = ra_donk
                            river_donker = ra_donk.get('player')
                            break # Encontrou o primeiro donk bet no river
                
                if river_donk_bet_action and river_donker:
                    # Agora verificar se pl_bbf foldou para este donk bet
                    idx_river_donk_bet = river_actions.index(river_donk_bet_action)
                    for ra_bbf_react in river_actions[idx_river_donk_bet + 1:]:
                        if ra_bbf_react.get('player') == pl_bbf: # Reação de pl_bbf ao donk
                            player_stats_data[pl_bbf].bbf_vs_donk_river_opportunities += 1
                            if ra_bbf_react.get('action') == 'folds':
                                player_stats_data[pl_bbf].bbf_vs_donk_river_actions += 1
                            break # Apenas a primeira reação de pl_bbf


        # --- Fold to River Bet por Linha (BBB, BXB, XBB, XXB) ---
        # PFA (hand.preflop_aggressor) é o jogador cujas ações de CBet estamos rastreando
        if pfa and river_actions: # Se PFA existe e houve ações no river
            pfa_flop_action_type = None # 'bets' ou 'checks'
            pfa_turn_action_type = None
            
            # Ação do PFA no Flop
            pfa_flop_first_opportunity_action = next((a for a in flop_actions if a.get('player') == pfa and a.get('action') in ['bets', 'checks']), None)
            if pfa_flop_first_opportunity_action:
                pfa_flop_action_type = pfa_flop_first_opportunity_action.get('action')

            # Ação do PFA no Turn (se ele agiu no flop)
            if pfa_flop_action_type:
                pfa_turn_first_opportunity_action = next((a for a in turn_actions if a.get('player') == pfa and a.get('action') in ['bets', 'checks']), None)
                if pfa_turn_first_opportunity_action:
                    pfa_turn_action_type = pfa_turn_first_opportunity_action.get('action')
            
            # Ação de Bet do PFA no River (se ele agiu no flop e turn)
            pfa_river_bet_action_obj = None
            if pfa_flop_action_type and pfa_turn_action_type:
                pfa_river_bet_action_obj = next((a for a in river_actions if a.get('player') == pfa and a.get('action') == 'bets'), None)

            if pfa_river_bet_action_obj: # PFA betou no river seguindo alguma linha
                line_code = ""
                line_code += "B" if pfa_flop_action_type == 'bets' else "X"
                line_code += "B" if pfa_turn_action_type == 'bets' else "X"
                line_code += "B" # River foi um Bet

                if line_code in ["BBB", "BXB", "XBB", "XXB"]:
                    bet_amount_river_line = pfa_river_bet_action_obj.get('amount',0)
                    pot_before_river_bet_line = pfa_river_bet_action_obj.get('pot_total_before_action',0)
                    if pot_before_river_bet_line > 0:
                        bet_pct_river_line = (bet_amount_river_line / pot_before_river_bet_line) * 100
                        size_group_river_line = player_stats_data[pfa].get_bet_size_group(bet_pct_river_line) # Usar stats do PFA para get_bet_size_group
                        
                        # Encontrar oponentes que enfrentaram este bet do PFA
                        idx_pfa_river_bet_line = river_actions.index(pfa_river_bet_action_obj)
                        players_faced_pfa_river_bet_line = set()
                        for react_to_pfa_line in river_actions[idx_pfa_river_bet_line + 1:]:
                            reactor_line = react_to_pfa_line.get('player')
                            if not reactor_line or reactor_line == pfa or reactor_line not in dealt_players: continue
                            if reactor_line in players_faced_pfa_river_bet_line: continue # Já teve oportunidade

                            # Verificar se o bet que o reactor enfrenta é o do PFA
                            if react_to_pfa_line.get('bet_faced_by_player_amount',0) == bet_amount_river_line and \
                               react_to_pfa_line.get('pot_when_bet_was_made',0) == pot_before_river_bet_line:
                                
                                players_faced_pfa_river_bet_line.add(reactor_line)
                                player_stats_data[reactor_line].fold_to_river_bet_by_line_opportunities_by_size[line_code][size_group_river_line] += 1
                                if react_to_pfa_line.get('action') == 'folds':
                                    player_stats_data[reactor_line].fold_to_river_bet_by_line_actions_by_size[line_code][size_group_river_line] += 1
        
        # --- Composição de Mãos no River (quando o PFA beta e é pago, e há showdown) ---
        if pfa and river_actions and hand.board_cards and len(hand.board_cards) >= 5 :
            # Reutilizar a lógica de linha do PFA acima
            pfa_flop_act_comp = None
            pfa_turn_act_comp = None
            pfa_river_bet_obj_comp = None
            pfa_river_bet_called_comp = False
            
            pfa_f_act = next((a.get('action') for a in flop_actions if a.get('player') == pfa and a.get('action') in ['bets', 'checks']), None)
            if pfa_f_act:
                pfa_flop_act_comp = pfa_f_act
                pfa_t_act = next((a.get('action') for a in turn_actions if a.get('player') == pfa and a.get('action') in ['bets', 'checks']), None)
                if pfa_t_act:
                    pfa_turn_act_comp = pfa_t_act
                    # PFA betou no river?
                    pfa_r_bet_obj = next((a for a in river_actions if a.get('player') == pfa and a.get('action') == 'bets'), None)
                    if pfa_r_bet_obj:
                        pfa_river_bet_obj_comp = pfa_r_bet_obj
                        # Alguém pagou o bet do PFA no river?
                        idx_pfa_r_bet_comp = river_actions.index(pfa_r_bet_obj)
                        for next_ra_obj_comp in river_actions[idx_pfa_r_bet_comp+1:]:
                            if next_ra_obj_comp.get('player') != pfa and next_ra_obj_comp.get('action') == 'calls':
                                # Verificar se o call corresponde ao bet do PFA
                                if next_ra_obj_comp.get('amount',0) == pfa_r_bet_obj.get('amount_to_call_overall_this_street', 0) - pfa_r_bet_obj.get('bets_this_street_by_player',{}).get(next_ra_obj_comp.get('player'),0) or \
                                   next_ra_obj_comp.get('amount',0) == pfa_r_bet_obj.get('last_bet_or_raise_amount_this_street',0) : # Simplificação
                                    pfa_river_bet_called_comp = True
                                    break
            
            if pfa_river_bet_obj_comp and pfa_river_bet_called_comp: # PFA betou river, foi pago
                line_code_comp = ""
                line_code_comp += "B" if pfa_flop_act_comp == 'bets' else "X"
                line_code_comp += "B" if pfa_turn_act_comp == 'bets' else "X"
                line_code_comp += "B" # River foi Bet

                if line_code_comp in ["BBB", "BXB", "XBB", "XXB"]:
                    # PFA mostrou as cartas?
                    showdown_pfa_action = next((sd_act for sd_act in showdown_actions if sd_act.get('player') == pfa and sd_act.get('action') == 'shows_hand'), None)
                    if showdown_pfa_action and showdown_pfa_action.get('description'):
                        showdown_pfa_desc = showdown_pfa_action.get('description')
                        bet_amount_river_comp = pfa_river_bet_obj_comp.get('amount',0)
                        pot_before_pfa_river_bet_comp = pfa_river_bet_obj_comp.get('pot_total_before_action', 0)

                        if pot_before_pfa_river_bet_comp > 0:
                            bet_percentage_river_comp = (bet_amount_river_comp / pot_before_pfa_river_bet_comp) * 100
                            size_group_river_comp = player_stats_data[pfa].get_bet_size_group(bet_percentage_river_comp)
                            hand_cat_river_comp = _get_simplified_hand_category_from_description(showdown_pfa_desc)

                            if hand_cat_river_comp != "desconhecido":
                                size_group_dict_comp = player_stats_data[pfa].river_bet_called_composition_by_line[line_code_comp][size_group_river_comp]
                                size_group_dict_comp[hand_cat_river_comp] += 1
                                size_group_dict_comp['total_showdowns'] += 1
    return player_stats_data


# --- Funções de Cache (podem ser removidas/adaptadas se o DB for a única fonte) ---
def load_cached_stats(filename):
    if not os.path.isfile(filename):
        return {}, set() # Retorna stats vazias e IDs processados vazios
    try:
        with open(filename, "rb") as f:
            data = pickle.load(f)
            # Assegura que 'stats' seja um defaultdict(player_stats_factory)
            raw_stats = data.get("stats", {})
            upgraded_stats = defaultdict(player_stats_factory)
            for player_name, old_ps_dict in raw_stats.items(): # Supondo que old_ps pode ser dict ou obj
                new_ps = PlayerStats(player_name) # Cria novo objeto
                if isinstance(old_ps_dict, PlayerStats): # Se for objeto, copia attrs
                    # Esta parte pode ser complexa se a estrutura do PlayerStats mudou.
                    # Por simplicidade, se for objeto, assume-se que é compatível.
                     # ATENÇÃO: Esta fusão pode ser problemática se a estrutura de PlayerStats mudou.
                     # Uma migração mais robusta seria necessária.
                    for attr, value in old_ps_dict.__dict__.items():
                        if hasattr(new_ps, attr):
                            current_val = getattr(new_ps, attr)
                            if isinstance(value, (int, float)) and isinstance(current_val, (int, float)):
                                setattr(new_ps, attr, current_val + value)
                            elif isinstance(value, dict) and isinstance(current_val, dict):
                                # Deep merge para dicts (como os by_size)
                                def _merge_dicts_recursive(d1, d2):
                                    for k, v2 in d2.items():
                                        if k in d1:
                                            v1 = d1[k]
                                            if isinstance(v1, dict) and isinstance(v2, dict):
                                                _merge_dicts_recursive(v1, v2)
                                            elif isinstance(v1, (int,float)) and isinstance(v2, (int,float)):
                                                d1[k] = v1 + v2
                                            # else: não mescla tipos diferentes ou não numéricos
                                        else:
                                            d1[k] = v2 # Adiciona novo
                                _merge_dicts_recursive(current_val, value)
                            # else: setattr(new_ps, attr, value) # Ou outra lógica de fusão
                elif isinstance(old_ps_dict, dict): # Se for dict (de um pickle antigo talvez)
                    # Tentar popular o novo objeto PlayerStats a partir do dict
                    # Isso requer que as chaves do dict correspondam aos atributos
                    for attr, value in old_ps_dict.items():
                         if hasattr(new_ps, attr): setattr(new_ps, attr, value)
                upgraded_stats[player_name] = new_ps

            processed = set(data.get("processed_hand_ids", []))
            return upgraded_stats, processed
    except Exception as e:
        print(f"Erro ao carregar cache '{filename}': {e}. Retornando dados vazios.")
        return defaultdict(player_stats_factory), set()

def save_cached_stats(filename, stats_data, processed_hand_ids):
    try:
        # Converter defaultdict para dict normal para serialização pickle mais segura
        stats_to_save = {name: ps for name, ps in stats_data.items()}
        with open(filename, "wb") as f:
            pickle.dump({"stats": stats_to_save, "processed_hand_ids": list(processed_hand_ids)}, f)
        print(f"Cache de estatísticas salvo em '{filename}'.")
    except Exception as e:
        print(f"Erro ao salvar cache '{filename}': {e}")

def merge_player_stats(base_stats_dict, new_stats_dict):
    """Mescla estatísticas de new_stats_dict em base_stats_dict."""
    for player_name, new_ps in new_stats_dict.items():
        if player_name not in base_stats_dict:
            base_stats_dict[player_name] = PlayerStats(player_name) # Cria se não existir

        base_ps = base_stats_dict[player_name]
        
        # Iterar sobre atributos numéricos simples
        for attr in dir(new_ps):
            if not attr.startswith("__") and not callable(getattr(new_ps, attr)):
                new_value = getattr(new_ps, attr)
                if hasattr(base_ps, attr):
                    base_value = getattr(base_ps, attr)
                    if isinstance(new_value, (int, float)) and isinstance(base_value, (int, float)):
                        setattr(base_ps, attr, base_value + new_value)
                    elif isinstance(new_value, dict) and isinstance(base_value, dict):
                        # Lógica de merge para defaultdicts (como os _by_size e _composition)
                        def _recursive_merge_defaultdicts(d1, d2):
                            for k, v2 in d2.items():
                                if isinstance(v2, defaultdict) or isinstance(v2, dict): # Checa se v2 é dict-like
                                    # d1[k] já será um defaultdict se d1 for defaultdict(factory)
                                    _recursive_merge_defaultdicts(d1[k], v2)
                                elif isinstance(v2, (int, float)):
                                    d1[k] += v2
                                # else: não mescla outros tipos
                        _recursive_merge_defaultdicts(base_value, new_value)
                    # elif base_value is None and new_value is not None: # Caso de inicialização
                        # setattr(base_ps, attr, new_value)

# --- Funções de Saída (main) ---
STAT_COLOR_RANGES = {
    "VPIP (%)": [{"max": 18, "class": "stat-tight"}, {"max": 28, "class": "stat-normal"}, {"max": 100, "class": "stat-loose"}],
    "PFR (%)": [{"max": 12, "class": "stat-passive"}, {"max": 20, "class": "stat-std-agg"}, {"max": 100, "class": "stat-very-agg"}],
    "3Bet PF (%)": [{"max": 4, "class": "stat-low"}, {"max": 8, "class": "stat-mid"}, {"max": 100, "class": "stat-high"}],
    "CBet Flop (%)": [{"max": 45, "class": "stat-low"}, {"max": 65, "class": "stat-mid"}, {"max": 100, "class": "stat-high"}],
    "Donk Bet Flop (%)": [{"max": 8, "class": "stat-low"}, {"max": 15, "class": "stat-mid"}, {"max": 100, "class": "stat-high"}],
    "Probe Bet Turn (%)": [{"max": 30, "class": "stat-low"}, {"max": 50, "class": "stat-mid"}, {"max": 100, "class": "stat-high"}],
}
def get_stat_color_class(stat_name, stat_value_numeric):
    if stat_value_numeric is None or math.isnan(stat_value_numeric) or math.isinf(stat_value_numeric): return ""
    
    # Classificação para blefes no river
    if stat_name.startswith("River ") and " Air (%)" in stat_name: # Ex: "River BBB 0-29% Air (%)"
        parts = stat_name.split(" ")
        if len(parts) >= 4: # River, Line, Size, Air, (%)
            size_group = parts[2]
            # Usar BLUFF_CLASS_THRESHOLDS do seu código original
            label = _classify_percentage(size_group, stat_value_numeric, BLUFF_CLASS_THRESHOLDS)
            color_map = {"under": "stat-tight", "gto": "stat-high", "over": "stat-normal"} # Ajuste as classes CSS conforme necessário
            return color_map.get(label, "")

    # Classificação para folds
    # Ex: "FTS Flop 0-29% (%)", "Fold CBet Flop IP 0-29% (%)", "Fold Donk Flop 0-29% (%)"
    if stat_name.startswith("FTS ") or stat_name.startswith("Fold CBet ") or stat_name.startswith("Fold Donk "):
        parts = stat_name.split(" ")
        size_group = None
        # Tentar extrair o size_group de forma mais robusta
        for part in parts:
            if "%" in part and "-" in part: # Provável size group como "0-29%"
                size_group = part
                break
        if size_group:
            # Usar FOLD_CLASS_THRESHOLDS
            label = _classify_percentage(size_group, stat_value_numeric, FOLD_CLASS_THRESHOLDS)
            color_map = {"under": "stat-tight", "gto": "stat-high", "over": "stat-normal"} # Ajuste as classes CSS
            return color_map.get(label, "")
            
    ranges = STAT_COLOR_RANGES.get(stat_name)
    if ranges:
        for r_color in ranges:
            if stat_value_numeric <= r_color["max"]: return r_color["class"]
    return ""


def main():
    input_filename = "historico_maos.txt"
    general_dir = "maos_gerais"
    html_output_filename = "estatisticas_poker_grid.html"
    cache_filename = "stats_cache.pkl" # Cache para PlayerStats objects e IDs processados

    conn = get_db_connection()
    create_tables(conn) # Cria tabelas se não existirem

    # Carregar stats cacheadas e IDs de mãos já processadas do pickle
    # No futuro, os IDs processados podem vir direto do DB, e as stats também.
    # Por enquanto, a lógica de cache de stats é mantida.
    stats_data_cache, processed_ids_cache = load_cached_stats(cache_filename)

    log_parts = []
    if os.path.isfile(input_filename):
        try:
            with open(input_filename, "r", encoding="utf-8") as f:
                log_parts.append(f.read())
        except Exception as e:
            print(f"Erro ao ler o arquivo '{input_filename}': {e}")
            # Não retorna, tenta ler do diretório geral
    else:
        print(f"Arquivo '{input_filename}' não encontrado.")

    if os.path.isdir(general_dir):
        for root_dir, _dirs, files in os.walk(general_dir):
            for fname in files:
                if fname.lower().endswith(".txt"):
                    fpath = os.path.join(root_dir, fname)
                    try:
                        with open(fpath, "r", encoding="utf-8") as f:
                            log_parts.append(f.read())
                    except Exception as e:
                        print(f"Erro ao ler '{fpath}': {e}")
    
    if not log_parts:
        print(f"ERRO: Nenhum arquivo de histórico encontrado em '{input_filename}' ou '{general_dir}'.")
        conn.close()
        return

    log_content = "\n".join(log_parts)
    if not log_content.strip():
        print("Nenhum conteúdo de log para processar.")
        conn.close()
        return

    print("Lendo histórico de mãos e salvando no banco de dados...")
    # parsed_hand_objects_for_stats: lista de objetos PokerHand parseados (novos ou não)
    # newly_inserted_in_db_count: contador de mãos efetivamente inseridas no DB nesta execução
    parsed_hand_objects_for_stats, newly_inserted_in_db_count = parse_poker_log_file_to_hands_and_save_to_db(log_content, conn)
    
    print(f"\n{newly_inserted_in_db_count} novas mãos inseridas no banco de dados.")
    
    # Temporariamente, ainda calculamos stats em memória usando os objetos retornados
    # para manter a funcionalidade de geração de HTML existente.
    # No futuro, esta parte será substituída por calculate_player_stats_from_db(conn)
    
    current_run_player_stats = defaultdict(player_stats_factory)
    if parsed_hand_objects_for_stats: # Se há mãos (novas ou repetidas) para calcular stats
        print("Calculando estatísticas dos jogadores (em memória, temporário)...")
        # A função calculate_player_stats espera uma lista de objetos PokerHand
        # Se você quiser calcular stats apenas para mãos NOVAS, filtre parsed_hand_objects_for_stats
        # Aqui, estamos recalculando para todas as mãos lidas (incluindo as que já estavam no DB)
        # para popular o stats_data_cache de forma incremental como antes.
        
        # Identificar mãos que NÃO estavam no cache de IDs para calcular stats apenas sobre elas
        # e depois mesclar com o cache.
        new_hands_for_stats_calc = [h for h in parsed_hand_objects_for_stats if h.hand_id not in processed_ids_cache]
        
        if new_hands_for_stats_calc:
            print(f"Calculando estatísticas para {len(new_hands_for_stats_calc)} mãos novas (não em cache)...")
            calculated_stats_for_new_hands = calculate_player_stats(new_hands_for_stats_calc)
            # Mesclar estas novas estatísticas com as do cache
            merge_player_stats(stats_data_cache, calculated_stats_for_new_hands)
            processed_ids_cache.update(h.hand_id for h in new_hands_for_stats_calc)
        else:
            print("Nenhuma mão nova para cálculo de estatísticas (todas já estavam no cache de IDs). Usando stats cacheadas.")
    else:
        print("Nenhuma mão lida para cálculo de estatísticas.")

    # stats_data_cache agora contém as estatísticas acumuladas
    if not stats_data_cache:
        print("Nenhuma estatística calculada.")
        conn.close()
        return

    # Salvar o cache atualizado (incluindo novos IDs processados e stats mescladas)
    save_cached_stats(cache_filename, stats_data_cache, processed_ids_cache)

    # Geração de HTML usa stats_data_cache
    print(f"\nSalvando estatísticas em HTML (Grid Layout) em '{html_output_filename}'...")
    try:
        with open(html_output_filename, "w", encoding="utf-8") as htmlfile:
            htmlfile.write("<!DOCTYPE html>\n<html lang='pt-br'>\n<head>\n  <meta charset='UTF-8'>\n")
            htmlfile.write("  <title>Poker Stats Grid</title>\n")
            htmlfile.write("  <style>\n")
            htmlfile.write("    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 10px; background-color: #2c3e50; color: #ecf0f1; font-size: 13px; }\n")
            htmlfile.write("    .hud-container { display: flex; flex-direction: column; align-items: center; }\n")
            htmlfile.write("    h1 { text-align: center; color: #ecf0f1; margin-bottom: 15px; }\n")
            htmlfile.write("    input#searchInput { width: 50%; padding: 10px; margin-bottom: 15px; border: 1px solid #7f8c8d; border-radius: 4px; background-color: #34495e; color: #ecf0f1; font-size: 0.9em; }\n")
            htmlfile.write("    .player-hud { border: 1px solid #7f8c8d; border-radius: 5px; margin-bottom: 15px; padding: 10px; background-color: #34495e; width: 95%; max-width: 1200px; display: none; }\n") # display: none por padrão
            htmlfile.write("    .player-hud h2 { margin-top: 0; border-bottom: 1px solid #7f8c8d; padding-bottom: 5px; color: #3498db; text-align: center; }\n")
            htmlfile.write("    .stat-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 10px; }\n")
            htmlfile.write("    .stat-block { background-color: #4a627a; padding: 10px; border-radius: 4px; }\n")
            htmlfile.write("    .stat-block h3 { margin-top: 0; color: #95a5a6; font-size: 0.95em; border-bottom: 1px solid #566f88; padding-bottom: 4px;}\n")
            htmlfile.write("    .stat-item { display: flex; justify-content: space-between; margin-bottom: 4px; font-size: 0.85em;}\n")
            htmlfile.write("    .stat-label { color: #bdc3c7; flex-basis: 70%; }\n") # Aumentado para dar mais espaço
            htmlfile.write("    .stat-value { color: #ecf0f1; font-weight: bold; text-align: right; flex-basis: 30%; }\n")
            # Classes de cor para valores de estatísticas
            htmlfile.write("    .stat-tight, .stat-passive, .stat-low { color: #e74c3c !important; } /* Vermelho para 'apertado', 'passivo', 'baixo' */\n")
            htmlfile.write("    .stat-normal, .stat-std-agg, .stat-mid { color: #2ecc71 !important; } /* Verde para 'normal', 'padrão agressivo', 'médio' */\n")
            htmlfile.write("    .stat-loose, .stat-very-agg, .stat-high { color: #3498db !important; } /* Azul para 'solto', 'muito agressivo', 'alto' */\n")
            htmlfile.write("  </style>\n</head>\n<body>\n  <div class='hud-container'>\n") # Container principal
            htmlfile.write("  <h1>Estatísticas de Poker - HUD View</h1>\n")
            htmlfile.write("  <input type='text' id='searchInput' onkeyup='searchPlayerHud()' placeholder='Buscar jogador para exibir HUD...'>\n")

            # Estrutura dos blocos de estatísticas para o HTML
            size_groups_display = ["0-29%", "30-45%", "46-56%", "57-70%", "80-100%", "101%+"] # Use os mesmos do seu PlayerStats
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
                # Blocos para stats por size
                "FTS Flop": [f"FTS Flop {sg} (%)" for sg in size_groups_display],
                "FTS Turn": [f"FTS Turn {sg} (%)" for sg in size_groups_display],
                "FTS River": [f"FTS River {sg} (%)" for sg in size_groups_display],
                "Call-Fold Turn": [f"CF Turn {sg} (%)" for sg in size_groups_display], # Call Flop, Fold Turn
                "Fold CBet Flop IP": [f"Fold CBet Flop IP {sg} (%)" for sg in size_groups_display],
                "Fold CBet Flop OOP": [f"Fold CBet Flop OOP {sg} (%)" for sg in size_groups_display],
                "Fold Donk Flop": [f"Fold Donk Flop {sg} (%)" for sg in size_groups_display],
                "Fold Donk Turn": [f"Fold Donk Turn {sg} (%)" for sg in size_groups_display],
                "Fold Donk River": [f"Fold Donk River {sg} (%)" for sg in size_groups_display],
                "Extra River": ["CCF vs Triple Barrel (%)", "BBF vs Donk River (%)"],
            }
            # Adicionar blocos de composição de river e FTS por linha dinamicamente
            for lt in line_types_display:
                stat_block_structure[f"FTS River {lt}"] = [f"FTS River {lt} {sg} (%)" for sg in size_groups_display]

            for lt in line_types_display:
                block_title_comp = f"River {lt} Composition"
                block_title_bv = f"River {lt} Bluff/Value"
                keys_comp = []
                keys_bv = []
                # Verificar se há dados para esta linha em algum jogador para evitar blocos vazios
                line_has_any_data_overall = False
                if stats_data_cache: # Verifica se há estatísticas
                    for p_stats_obj in stats_data_cache.values():
                        line_data_check = p_stats_obj.river_bet_called_composition_by_line.get(lt)
                        if line_data_check:
                            for sg_data_dict_check in line_data_check.values():
                                if sg_data_dict_check.get('total_showdowns',0) > 0:
                                    line_has_any_data_overall = True; break
                            if line_has_any_data_overall: break
                
                if line_has_any_data_overall:
                    for sg in size_groups_display:
                        # Apenas adiciona se houver showdowns para este size group específico na linha
                        # (Pode ser otimizado checando dentro do loop de jogadores se é melhor)
                        for hc_display in hand_cat_display_map.values():
                            keys_comp.append(f"River {lt} {sg} {hc_display} (%)")
                        keys_bv.append(f"River {lt} {sg} Bluff (%)")
                        keys_bv.append(f"River {lt} {sg} Value (%)")
                        keys_bv.append(f"River {lt} {sg} Bluff vs MDF")
                    if keys_comp : stat_block_structure[block_title_comp] = keys_comp
                    if keys_bv : stat_block_structure[block_title_bv] = keys_bv
            
            sorted_player_names = sorted(stats_data_cache.keys())

            for player_name_html in sorted_player_names:
                if player_name_html not in stats_data_cache: continue # Segurança
                player_stat_obj_html = stats_data_cache[player_name_html]
                stat_dict_display_html = player_stat_obj_html.to_dict_display()

                htmlfile.write(f"  <div class='player-hud' id='hud-{player_name_html.replace(' ', '-').replace('.', '')}'>\n")
                htmlfile.write(f"    <h2>{player_name_html}</h2>\n")
                htmlfile.write("    <div class='stat-grid'>\n")

                for block_title_html, stat_keys_in_block_html in stat_block_structure.items():
                    # Filtrar chaves que realmente têm dados para este jogador para este bloco
                    active_stat_keys_for_player_in_block_html = []
                    for sk_html in stat_keys_in_block_html:
                        if sk_html in stat_dict_display_html:
                            val_display_html = str(stat_dict_display_html[sk_html])
                            # Mostrar bloco se for Geral/PF ou se tiver dados (não for "0.0% (0/0)")
                            # Ou se for um bloco de composição de river e a linha tiver dados (já filtrado antes)
                            always_show_block_type_html = block_title_html == "Geral" or \
                                                     block_title_html.startswith("PF") or \
                                                     block_title_html.startswith("FTS") or \
                                                     (block_title_html.startswith("River") and "Composition" in block_title_html) or \
                                                     (block_title_html.startswith("River") and "Bluff/Value" in block_title_html)


                            has_data_html = not (val_display_html.startswith("0.0%") and val_display_html.endswith("(0/0)"))
                            
                            # Se o bloco for de composição, a lógica de `line_has_any_data_overall` já garante que o bloco só é criado se houver dados.
                            # Então, para esses blocos, podemos adicionar todas as suas chaves se o bloco existir.
                            if (block_title_html.startswith("River ") and ("Composition" in block_title_html or "Bluff/Value" in block_title_html)) or \
                               always_show_block_type_html or has_data_html:
                                active_stat_keys_for_player_in_block_html.append(sk_html)
                    
                    if not active_stat_keys_for_player_in_block_html: continue # Pula bloco se não houver stats ativas para ele

                    htmlfile.write("      <div class='stat-block'>\n")
                    htmlfile.write(f"        <h3>{block_title_html}</h3>\n")
                    for stat_key_html in active_stat_keys_for_player_in_block_html:
                        display_value_html = stat_dict_display_html.get(stat_key_html, "-")
                        numeric_val_for_color_html = player_stat_obj_html.get_raw_stat_value(stat_key_html) # Passa a chave original
                        
                        color_class_html = get_stat_color_class(stat_key_html, numeric_val_for_color_html if isinstance(numeric_val_for_color_html, (int, float)) else 0.0)
                        
                        label_html = stat_key_html.replace(' (%)','').replace('FTS ','FTS ') # Limpa um pouco o label
                        # Para blocos de composição, simplificar mais o label
                        if block_title_html.startswith("River") and ("Composition" in block_title_html or "Bluff/Value" in block_title_html) :
                            parts_label = stat_key_html.split(" ")
                            # Ex: "River BBB 0-29% Air (%)" -> "0-29% Air"
                            # Ex: "River BBB 0-29% Bluff vs MDF" -> "0-29% Bluff vs MDF"
                            if len(parts_label) >= 4:
                                if parts_label[-1] == "(%)":
                                    label_html = f"{parts_label[-3]} {parts_label[-2]}" # Size HandCat
                                elif parts_label[-2] == "vs" and parts_label[-1] == "MDF":
                                     label_html = f"{parts_label[-4]} {parts_label[-3]} vs MDF" # Size Bluff vs MDF
                                else:
                                     label_html = " ".join(parts_label[2:]) # Fallback
                        
                        htmlfile.write(f"        <div class='stat-item'>\n")
                        htmlfile.write(f"          <span class='stat-label'>{label_html}</span>\n")
                        htmlfile.write(f"          <span class='stat-value {color_class_html}'>{display_value_html}</span>\n")
                        htmlfile.write("        </div>\n")
                    htmlfile.write("      </div>\n")
                htmlfile.write("    </div>\n") # Fim stat-grid
                htmlfile.write("  </div>\n") # Fim player-hud

            # Script JS para a busca
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
                huds[i].style.display = "block"; // Mostra o HUD do jogador
            } else {
                huds[i].style.display = "none"; // Esconde
            }
        }
      }
      // Se o campo de busca estiver vazio, esconde todos
      if (filter === "") {
          for (i = 0; i < huds.length; i++) {
              huds[i].style.display = "none";
          }
      }
    }
    // Para garantir que todos os HUDs estejam escondidos ao carregar a página
    document.addEventListener('DOMContentLoaded', function() {
        var huds = document.getElementsByClassName("player-hud");
        for (var i = 0; i < huds.length; i++) {
            huds[i].style.display = "none";
        }
    });
  </script>
""")
            htmlfile.write("</div>\n</body>\n</html>") # Fim hud-container e html
        print(f"Estatísticas HTML (Grid) salvas com sucesso em '{html_output_filename}'.")

        # Geração do estatisticas_resumidas.html (usando stats_data_cache)
        summary_filename = "estatisticas_resumidas.html"
        try:
            with open(summary_filename, "w", encoding="utf-8") as sf:
                sf.write("<!DOCTYPE html>\n<html lang='pt-br'>\n<head>\n  <meta charset='UTF-8'>\n")
                sf.write("  <title>Estatísticas Resumidas</title>\n")
                sf.write("  <style>\n")
                sf.write("    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #2c3e50; color: #ecf0f1; margin: 10px; }\n")
                sf.write("    #searchInputResumo { width: 50%; padding: 10px; margin-bottom: 15px; border: 1px solid #7f8c8d; border-radius: 4px; background-color: #34495e; color: #ecf0f1; }\n")
                sf.write("    .player-summary { border: 1px solid #7f8c8d; border-radius: 5px; margin-bottom: 15px; padding: 10px; background-color: #34495e; display: none; }\n") # display: none por padrão
                sf.write("    .player-summary h2 { margin-top: 0; color: #3498db; text-align: center; }\n")
                sf.write("    .stat-line { margin: 3px 0; }\n")
                # Classes de cor para resumo (podem ser as mesmas do grid ou diferentes)
                sf.write("    .stat-tight { color: #e74c3c !important; }\n")
                sf.write("    .stat-high { color: #2ecc71 !important; }\n") # No resumo, GTO é verde (high)
                sf.write("    .stat-normal { color: #3498db !important; }\n") # E over é azul (normal)
                sf.write("  </style>\n</head>\n<body>\n")
                sf.write("  <h1>Estatísticas Resumidas</h1>\n")
                sf.write("  <input type='text' id='searchInputResumo' onkeyup='searchResumo()' placeholder='Buscar jogador...'>\n")

                for player_name_sum in sorted(stats_data_cache.keys()):
                    stat_dict_display_sum = stats_data_cache[player_name_sum].to_dict_display()
                    sf.write(f"  <div class='player-summary' id='resumo-{player_name_sum.replace(' ', '-').replace('.', '')}'>\n")
                    sf.write(f"    <h2>{player_name_sum}</h2>\n")
                    for k_sum, v_sum in stat_dict_display_sum.items():
                        # Filtrar quais stats vão para o resumo
                        # Exemplo: apenas as que têm classificação (Under, GTO, Over)
                        # Ou uma lista pré-definida de stats importantes
                        value_str_sum = str(v_sum)
                        upper_val_sum = value_str_sum.upper()
                        
                        # Mostrar stats que contêm classificações (Under, GTO, Over)
                        # E algumas stats básicas como VPIP, PFR, Hands Played
                        show_this_stat_in_summary = False
                        if any(lbl_sum in upper_val_sum for lbl_sum in ['UNDER', 'GTO', 'OVER']):
                            show_this_stat_in_summary = True
                        elif k_sum in ["Hands Played", "VPIP (%)", "PFR (%)", "3Bet PF (%)", "Fold to PF 3Bet (%)", "CBet Flop (%)", "Fold to Flop CBet (%)"]:
                            show_this_stat_in_summary = True
                        
                        if show_this_stat_in_summary:
                            label_sum = k_sum.replace('CF Turn', 'C/F Turn').replace('Bluff vs MDF', 'Bluff vs MDF')
                            label_sum = label_sum.replace('River ', '').replace(' (%)', '') # Simplifica
                            
                            color_class_sum = ''
                            numeric_val_sum_color = stats_data_cache[player_name_sum].get_raw_stat_value(k_sum)
                            if isinstance(numeric_val_sum_color, (int, float)):
                                color_class_sum = get_stat_color_class(k_sum, numeric_val_sum_color)
                            
                            # Sobrescrever classe de cor se houver label explícito (Under, GTO, Over)
                            if 'UNDER' in upper_val_sum: color_class_sum = 'stat-tight'
                            elif 'OVER' in upper_val_sum: color_class_sum = 'stat-normal' # Over é azul no resumo
                            elif 'GTO' in upper_val_sum: color_class_sum = 'stat-high'   # GTO é verde no resumo
                                
                            sf.write(f"    <div class='stat-line {color_class_sum}'>{label_sum}: {v_sum}</div>\n")
                    sf.write("  </div>\n")
                
                # Script JS para busca no resumo
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
      if (filter === '') { // Se vazio, esconde todos
        for (var i = 0; i < divs.length; i++) { divs[i].style.display = 'none'; }
      }
    }
    // Esconde todos os resumos ao carregar
    document.addEventListener('DOMContentLoaded', function() {
      var divs = document.getElementsByClassName('player-summary');
      for (var i = 0; i < divs.length; i++) { divs[i].style.display = 'none'; }
    });
  </script>
</body>
</html>""")
            print(f"Resumo salvo em '{summary_filename}'.")
        except Exception as e:
            print(f"Erro ao salvar resumo '{summary_filename}': {e}")
    except Exception as e:
        print(f"ERRO ao salvar o arquivo HTML (Grid) '{html_output_filename}': {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close() # Fecha a conexão com o DB

if __name__ == "__main__":
    main()
