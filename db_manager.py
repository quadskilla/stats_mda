# db_manager.py
import sqlite3

DB_NAME = "poker_data.db"

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
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
    # Índices (Exemplo, adicione mais conforme necessário)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_actions_player_street_type ON actions (player_id, street, action_type);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_actions_hand_sequence ON actions (hand_db_id, action_sequence);")
    # Índices adicionais para acelerar consultas complexas de Pré-Flop
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_actions_hand_player ON actions (hand_db_id, player_id);"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_actions_hand_street_type ON actions (hand_db_id, street, action_type);"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_hand_players_hand_player ON hand_players (hand_db_id, player_id);"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_hand_players_hand_position ON hand_players (hand_db_id, position);"
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hands_pfa ON hands (preflop_aggressor_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_hands_history_id ON hands (hand_history_id);") # Muito importante
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_players_name ON players (player_name);")

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
            # Commit será feito em lote ou pelo chamador de save_hand_to_db
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            cursor.execute("SELECT player_id FROM players WHERE player_name = ?", (player_name,))
            row = cursor.fetchone()
            return row['player_id'] if row else None

def save_hand_to_db(conn, hand_obj): # Recebe um objeto PokerHand
    cursor = conn.cursor()
    player_name_to_id_map = {}
    all_player_names_in_hand = set()

    if hand_obj.hero_name: all_player_names_in_hand.add(hand_obj.hero_name)
    for seat_info in hand_obj.player_seat_info.values():
        if seat_info['name']: all_player_names_in_hand.add(seat_info['name'])
    for action_data in hand_obj.actions:
        if action_data.get('player'): all_player_names_in_hand.add(action_data['player'])
    
    if hand_obj.preflop_aggressor: all_player_names_in_hand.add(hand_obj.preflop_aggressor)
    if hand_obj.flop_aggressor: all_player_names_in_hand.add(hand_obj.flop_aggressor)
    if hand_obj.turn_aggressor: all_player_names_in_hand.add(hand_obj.turn_aggressor)
    if hand_obj.river_aggressor: all_player_names_in_hand.add(hand_obj.river_aggressor)

    for name in filter(None, all_player_names_in_hand):
        player_id = get_or_create_player_id(conn, name)
        if player_id is not None: # Adicionado para evitar erro se get_or_create_player_id falhar
            player_name_to_id_map[name] = player_id


    hero_db_id = player_name_to_id_map.get(hand_obj.hero_name)
    pfa_id = player_name_to_id_map.get(hand_obj.preflop_aggressor)
    fa_id = player_name_to_id_map.get(hand_obj.flop_aggressor)
    ta_id = player_name_to_id_map.get(hand_obj.turn_aggressor)
    ra_id = player_name_to_id_map.get(hand_obj.river_aggressor)
    final_pot = hand_obj.current_pot_total

    try:
        cursor.execute("""
            INSERT INTO hands (hand_history_id, tournament_id, datetime_str, table_id, button_seat_num, hero_id, big_blind_amount, board_cards,
                             preflop_aggressor_id, flop_aggressor_id, turn_aggressor_id, river_aggressor_id, pot_total_at_showdown)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (hand_obj.hand_id, hand_obj.tournament_id, hand_obj.datetime_str, hand_obj.table_id, hand_obj.button_seat_num, hero_db_id,
              hand_obj.big_blind_amount, ' '.join(hand_obj.board_cards) if hand_obj.board_cards else None,
              pfa_id, fa_id, ta_id, ra_id, final_pot))
        hand_db_id = cursor.lastrowid
    except sqlite3.IntegrityError:
        # print(f"Mão {hand_obj.hand_id} já existe no DB. Pulando inserção detalhada.")
        return None 

    for seat_num, seat_info in hand_obj.player_seat_info.items():
        if seat_info['name'] and seat_info['name'] in player_name_to_id_map : # Verifica se o ID foi obtido
            player_db_id = player_name_to_id_map[seat_info['name']]
            position = hand_obj.player_positions.get(seat_info['name'])
            cards = hand_obj.hole_cards.get(seat_info['name'])
            try:
                cursor.execute("""
                    INSERT INTO hand_players (hand_db_id, player_id, seat_num, initial_chips, position, hole_cards)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (hand_db_id, player_db_id, seat_num, seat_info['chips'], position, cards))
            except sqlite3.IntegrityError:
                pass 

    for i, action_data in enumerate(hand_obj.actions):
        player_name_for_action = action_data.get('player')
        player_db_id_for_action = player_name_to_id_map.get(player_name_for_action) if player_name_for_action else None
        amount_val = action_data.get('amount')
        total_bet_val = action_data.get('total_bet')
        
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
              pot_total_before, amount_to_call, bet_faced, pot_when_bet))
    
    return hand_db_id # Retorna o ID da mão inserida

def check_hand_exists(conn, hand_history_id):
    cursor = conn.cursor()
    cursor.execute("SELECT hand_db_id FROM hands WHERE hand_history_id = ?", (hand_history_id,))
    return cursor.fetchone() is not None