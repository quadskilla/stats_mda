# stats_calculator_flop.py
import sqlite3
from collections import defaultdict
# from .stats_calculator import PlayerStats (se PlayerStats estiver em stats_calculator.py principal)

def calculate_flop_stats_for_player(ps, cursor: sqlite3.Cursor, player_id: int):
    """
    Calcula e preenche as estatísticas de Flop para o objeto PlayerStats (ps).
    """
    if ps.hands_played == 0: return

    # --- CBet Flop (Geral, IP, OOP) ---
    # Geral já foi calculado no stats_calculator.py principal (ps.cbet_flop_opportunities, ps.cbet_flop_actions)
    # Para IP/OOP, precisamos determinar a posição relativa do PFA.
    # Isso é complexo de fazer puramente com SQL sem mais dados sobre a ordem de ação na street.
    # Assumindo que você tem uma forma de determinar se PFA é IP/OOP no Flop.
    # Se 'hands' tivesse uma coluna 'pfa_is_ip_on_flop' (boolean), seria mais fácil.
    # Por ora, vamos calcular o geral, e IP/OOP pode precisar de lógica adicional ou ser aproximado.

    # --- CBet Flop IP ---
    # Oportunidade: PFA é o jogador, está no flop, é IP, ninguém betou antes.
    # Ação: PFA beta.
    # Consulta Placeholder (requer lógica de determinação de IP no Flop):
    # ps.cbet_flop_ip_opportunities = 0
    # ps.cbet_flop_ip_actions = 0

    # --- CBet Flop OOP ---
    # Similar ao IP.
    # ps.cbet_flop_oop_opportunities = 0
    # ps.cbet_flop_oop_actions = 0


    # --- Fold to Flop CBet (Geral, IP, OOP) ---
    # Geral já foi calculado no stats_calculator.py principal
    # Para IP/OOP, o jogador (não PFA) enfrenta CBet e está IP/OOP em relação ao PFA.
    # Placeholder (requer lógica de IP/OOP):
    # ps.fold_to_flop_cbet_ip_opportunities = 0
    # ps.fold_to_flop_cbet_ip_actions = 0
    # ps.fold_to_flop_cbet_oop_opportunities = 0
    # ps.fold_to_flop_cbet_oop_actions = 0

    # --- Fold to Flop CBet por Size e Posição (IP/OOP) ---
    # Este é ainda mais granular.
    # Exemplo para Fold to Flop CBet IP por Size:
    cursor.execute("""
        WITH PFAIsNOTPlayer AS (SELECT hand_db_id, preflop_aggressor_id FROM hands WHERE preflop_aggressor_id IS NOT NULL AND preflop_aggressor_id != ?),
        PFAMadeCBet AS (
            SELECT DISTINCT pnp.hand_db_id, cbet_a.action_sequence as cbet_seq, cbet_a.player_id as pfa_id,
                   cbet_a.bet_faced_by_player_amount as cbet_amount_faced_by_next_player, /* Na verdade, é o 'amount' da cbet */
                   cbet_a.amount as cbet_value, /* Valor do bet */
                   cbet_a.pot_when_bet_was_made as pot_at_cbet_time /* Na verdade, é pot_total_before_action da cbet */
            FROM PFAIsNOTPlayer pnp JOIN actions cbet_a ON pnp.hand_db_id = cbet_a.hand_db_id
            WHERE cbet_a.player_id = pnp.preflop_aggressor_id AND cbet_a.street = 'Flop' AND cbet_a.action_type = 'bets'
            AND NOT EXISTS (SELECT 1 FROM actions pre_cbet_a WHERE pre_cbet_a.hand_db_id = pnp.hand_db_id AND pre_cbet_a.street = 'Flop'
                            AND pre_cbet_a.action_type IN ('bets', 'raises') AND pre_cbet_a.action_sequence < cbet_a.action_sequence)
        ),
        PlayerFacedCBetIP AS ( -- Oportunidades para o jogador (IP) reagir à CBet
            SELECT DISTINCT pfmc.hand_db_id,
                   CAST(ROUND((player_react.bet_faced_by_player_amount * 100.0) / NULLIF(player_react.pot_when_bet_was_made, 0)) AS INTEGER) as bet_perc,
                   player_react.action_type as reaction_action_type
            FROM PFAMadeCBet pfmc
            JOIN actions player_react ON pfmc.hand_db_id = player_react.hand_db_id
            JOIN hand_players hp_player ON player_react.hand_db_id = hp_player.hand_db_id AND player_react.player_id = hp_player.player_id
            JOIN hand_players hp_pfa ON pfmc.hand_db_id = hp_pfa.hand_db_id AND pfmc.pfa_id = hp_pfa.player_id
            WHERE player_react.player_id = ? AND player_react.street = 'Flop'
              AND player_react.action_sequence > pfmc.cbet_seq AND player_react.bet_faced_by_player_amount > 0
              AND hp_player.seat_num > hp_pfa.seat_num /* Aproximação MUITO SIMPLES para IP (BTN vs Blinds, CO vs BTN etc) - PRECISA MELHORAR */
              /* Para IP/OOP correto, você precisaria da ordem de ação exata dos envolvidos */
        )
        SELECT bet_perc, reaction_action_type, COUNT(*) as count
        FROM PlayerFacedCBetIP
        GROUP BY bet_perc, reaction_action_type
    """, (player_id, player_id)) # PFAIsNOTPlayer (player_id), PlayerFacedCBetIP (player_id)

    for row in cursor.fetchall():
        sg = ps.get_bet_size_group(row['bet_perc'] if row['bet_perc'] is not None else None)
        if sg != "N/A":
            ps.fold_to_flop_cbet_ip_opportunities_by_size[sg] += row['count']
            if row['reaction_action_type'] == 'folds':
                ps.fold_to_flop_cbet_ip_actions_by_size[sg] += row['count']
    # Repetir lógica similar para OOP, ajustando a condição de posição.

    # --- Donk Bet Flop ---
    # Oportunidade: Jogador NÃO é PFA, PFA ainda não agiu no flop, e jogador está OOP ao PFA (ou é o primeiro a agir).
    # Ação: Jogador beta.
    cursor.execute("""
        WITH PFANotPlayer AS (SELECT hand_db_id, preflop_aggressor_id FROM hands WHERE preflop_aggressor_id IS NOT NULL AND preflop_aggressor_id != ?),
        DonkOpps AS (
            SELECT DISTINCT pnp.hand_db_id
            FROM PFANotPlayer pnp
            JOIN actions player_act ON pnp.hand_db_id = player_act.hand_db_id
            WHERE player_act.player_id = ? AND player_act.street = 'Flop'
              AND player_act.action_type IN ('bets', 'checks') -- Chance de agir (betar ou checkar)
              AND NOT EXISTS ( -- PFA não agiu ainda no flop
                  SELECT 1 FROM actions pfa_act WHERE pfa_act.hand_db_id = pnp.hand_db_id AND pfa_act.street = 'Flop'
                    AND pfa_act.player_id = pnp.preflop_aggressor_id AND pfa_act.action_sequence < player_act.action_sequence
              )
              AND NOT EXISTS ( -- Ninguém betou/raisou antes do jogador nesta street
                  SELECT 1 FROM actions pre_player_bet WHERE pre_player_bet.hand_db_id = pnp.hand_db_id AND pre_player_bet.street = 'Flop'
                    AND pre_player_bet.action_type IN ('bets', 'raises') AND pre_player_bet.action_sequence < player_act.action_sequence
              )
              -- Adicional: Lógica para verificar se está OOP ao PFA se PFA ainda estiver na mão. Complexo.
              -- Simplificação: Qualquer bet antes do PFA agir é um Donk Potencial.
        ),
        DonkActs AS (
            SELECT DISTINCT dopps.hand_db_id
            FROM DonkOpps dopps
            JOIN actions donk_b ON dopps.hand_db_id = donk_b.hand_db_id
            WHERE donk_b.player_id = ? AND donk_b.street = 'Flop' AND donk_b.action_type = 'bets'
        )
        SELECT (SELECT COUNT(*) FROM DonkOpps) as opps, (SELECT COUNT(*) FROM DonkActs) as acts
    """, (player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.donk_bet_flop_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.donk_bet_flop_actions = res['acts'] if res and res['acts'] is not None else 0

    # --- Fold to Donk Flop ---
    # Oportunidade: Jogador é PFA e enfrenta um Donk Bet.
    # Ação: PFA folda.
    cursor.execute("""
        WITH PFAIsPlayer AS (SELECT hand_db_id FROM hands WHERE preflop_aggressor_id = ?),
        FacedDonkBet AS ( -- Mãos onde PFA (jogador) enfrentou um donk bet
            SELECT DISTINCT pfa_ip.hand_db_id, donk_action.action_sequence as donk_seq
            FROM PFAIsPlayer pfa_ip
            JOIN actions donk_action ON pfa_ip.hand_db_id = donk_action.hand_db_id
            WHERE donk_action.street = 'Flop' AND donk_action.action_type = 'bets'
              AND donk_action.player_id != ? -- Donk por outro jogador
              AND NOT EXISTS ( -- Garante que PFA (jogador) não agiu antes do donk
                  SELECT 1 FROM actions pfa_prev_act WHERE pfa_prev_act.hand_db_id = pfa_ip.hand_db_id
                    AND pfa_prev_act.street = 'Flop' AND pfa_prev_act.player_id = ?
                    AND pfa_prev_act.action_sequence < donk_action.action_sequence
              )
        ),
        FoldedToDonkActs AS (
            SELECT DISTINCT fdb.hand_db_id
            FROM FacedDonkBet fdb
            JOIN actions pfa_fold_act ON fdb.hand_db_id = pfa_fold_act.hand_db_id
            WHERE pfa_fold_act.player_id = ? AND pfa_fold_act.street = 'Flop' AND pfa_fold_act.action_type = 'folds'
            AND pfa_fold_act.action_sequence > fdb.donk_seq
        )
        SELECT (SELECT COUNT(*) FROM FacedDonkBet) as opps, (SELECT COUNT(*) FROM FoldedToDonkActs) as acts
    """, (player_id, player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.fold_to_donk_bet_flop_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.fold_to_donk_bet_flop_actions = res['acts'] if res and res['acts'] is not None else 0

    # --- Bet vs Missed CBet Flop ---
    # Oportunidade: PFA checkou no flop, e é a vez do jogador (que não é PFA).
    # Ação: Jogador beta.
    # ... (Consulta SQL Similar, mas PFA deve ter checkado)

    # --- Fold to Bet vs Missed CBet Flop ---
    # Oportunidade: Jogador é PFA, checkou no flop, outro jogador betou, e é a vez do PFA.
    # Ação: PFA folda.
    # ...

    # --- Check-Raise Flop ---
    # Oportunidade: Jogador deu check, outro jogador betou, e é a vez do jogador novamente.
    # Ação: Jogador faz um raise.
    # ...

    # --- Fold to XR Flop ---
    # Oportunidade: Jogador betou, outro jogador fez Check-Raise, e é a vez do jogador.
    # Ação: Jogador folda.
    # ...

    # --- FTS Flop por Size (já parcialmente coberto no stats_calculator.py principal, pode refinar aqui) ---
    # A consulta no `calculate_stats_for_single_player` principal já faz isso.

    # --- Fold to Donk Flop por Size ---
    # Similar ao FTS, mas filtrando para situações de Donk.
    cursor.execute("""
        WITH PFAIsPlayer AS (SELECT hand_db_id FROM hands WHERE preflop_aggressor_id = ?),
        FacedDonkBetWithSize AS (
            SELECT DISTINCT pfa_ip.hand_db_id,
                   CAST(ROUND((pfa_react.bet_faced_by_player_amount * 100.0) / NULLIF(pfa_react.pot_when_bet_was_made, 0)) AS INTEGER) as bet_perc,
                   pfa_react.action_type as reaction_action_type
            FROM PFAIsPlayer pfa_ip
            JOIN actions donk_action ON pfa_ip.hand_db_id = donk_action.hand_db_id
            JOIN actions pfa_react ON pfa_ip.hand_db_id = pfa_react.hand_db_id AND pfa_react.player_id = ?
            WHERE donk_action.street = 'Flop' AND donk_action.action_type = 'bets' AND donk_action.player_id != ?
              AND NOT EXISTS (SELECT 1 FROM actions pfa_prev_act WHERE pfa_prev_act.hand_db_id = pfa_ip.hand_db_id AND pfa_prev_act.street = 'Flop' AND p_prev_act.player_id = ? AND p_prev_act.action_sequence < donk_action.action_sequence)
              AND pfa_react.street = 'Flop' AND pfa_react.action_sequence > donk_action.action_sequence
              AND pfa_react.bet_faced_by_player_amount > 0 /* PFA (jogador) está enfrentando o donk bet */
        )
        SELECT bet_perc, reaction_action_type, COUNT(*) as count
        FROM FacedDonkBetWithSize
        GROUP BY bet_perc, reaction_action_type
    """, (player_id, player_id, player_id, player_id, player_id)) # Cuidado com a ordem dos player_id
    for row in cursor.fetchall():
        sg = ps.get_bet_size_group(row['bet_perc'] if row['bet_perc'] is not None else None)
        if sg != "N/A":
            ps.fold_to_donk_bet_flop_opportunities_by_size[sg] += row['count']
            if row['reaction_action_type'] == 'folds':
                ps.fold_to_donk_bet_flop_actions_by_size[sg] += row['count']