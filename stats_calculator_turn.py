# stats_calculator_turn.py
import sqlite3
from collections import defaultdict
# from .stats_calculator import PlayerStats, _get_simplified_hand_category_from_description, FOLD_CLASS_THRESHOLDS, _classify_percentage
# Se PlayerStats e outras constantes/funções estiverem no stats_calculator.py principal

def calculate_turn_stats_for_player(ps, cursor: sqlite3.Cursor, player_id: int):
    """
    Calcula e preenche as estatísticas de Turn para o objeto PlayerStats (ps).
    """
    if ps.hands_played == 0: return

    # --- CBet Turn ---
    # Oportunidade: Jogador foi o Flop Aggressor (FA), chegou no turn, ninguém betou antes dele no turn.
    # Ação: Jogador beta no turn.
    cursor.execute("""
        WITH FlopAggressorIsPlayer AS (
            SELECT hand_db_id FROM hands WHERE flop_aggressor_id = ?
        ),
        CBetTurnOpps AS (
            SELECT DISTINCT faip.hand_db_id
            FROM FlopAggressorIsPlayer faip
            JOIN actions ta ON faip.hand_db_id = ta.hand_db_id
            WHERE ta.player_id = ? AND ta.street = 'Turn'
              AND ta.action_type IN ('bets', 'checks') -- Oportunidade de agir
              AND NOT EXISTS (SELECT 1 FROM actions pre_turn_bet 
                              WHERE pre_turn_bet.hand_db_id = faip.hand_db_id AND pre_turn_bet.street = 'Turn'
                              AND pre_turn_bet.action_type IN ('bets', 'raises') 
                              AND pre_turn_bet.action_sequence < ta.action_sequence)
        ),
        CBetTurnActs AS (
            SELECT DISTINCT cbo.hand_db_id
            FROM CBetTurnOpps cbo
            JOIN actions ta_bet ON cbo.hand_db_id = ta_bet.hand_db_id
            WHERE ta_bet.player_id = ? AND ta_bet.street = 'Turn' AND ta_bet.action_type = 'bets'
            AND ta_bet.action_sequence = (SELECT MIN(act_seq.action_sequence) FROM actions act_seq 
                                          WHERE act_seq.hand_db_id = cbo.hand_db_id AND act_seq.street = 'Turn' 
                                          AND act_seq.player_id = ? AND act_seq.action_type IN ('bets', 'checks'))
        )
        SELECT (SELECT COUNT(*) FROM CBetTurnOpps) as opps, (SELECT COUNT(*) FROM CBetTurnActs) as acts
    """, (player_id, player_id, player_id, player_id)) # Player ID para flop_aggressor_id e para player_id na ação
    res = cursor.fetchone()
    ps.cbet_turn_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.cbet_turn_actions = res['acts'] if res and res['acts'] is not None else 0

    # --- Fold to Turn CBet ---
    # Oportunidade: Jogador NÃO foi FA, FA betou no Turn (CBet Turn), é a vez do jogador.
    # Ação: Jogador folda.
    cursor.execute("""
        WITH FAIsNOTPlayer AS (
            SELECT hand_db_id, flop_aggressor_id FROM hands 
            WHERE flop_aggressor_id IS NOT NULL AND flop_aggressor_id != ?
        ),
        FAMadeCBetTurn AS (
            SELECT DISTINCT fainp.hand_db_id, cbet_ta.action_sequence as cbet_turn_seq, fainp.flop_aggressor_id as fa_id
            FROM FAIsNOTPlayer fainp
            JOIN actions cbet_ta ON fainp.hand_db_id = cbet_ta.hand_db_id
            WHERE cbet_ta.player_id = fainp.flop_aggressor_id AND cbet_ta.street = 'Turn' AND cbet_ta.action_type = 'bets'
            AND NOT EXISTS (SELECT 1 FROM actions pre_cbet_ta WHERE pre_cbet_ta.hand_db_id = fainp.hand_db_id AND pre_cbet_ta.street = 'Turn'
                            AND pre_cbet_ta.action_type IN ('bets', 'raises') AND pre_cbet_ta.action_sequence < cbet_ta.action_sequence)
        ),
        FacedTurnCBetOpps AS (
            SELECT DISTINCT famct.hand_db_id
            FROM FAMadeCBetTurn famct
            JOIN actions player_turn_act ON famct.hand_db_id = player_turn_act.hand_db_id
            WHERE player_turn_act.player_id = ? AND player_turn_act.street = 'Turn'
              AND player_turn_act.action_sequence > famct.cbet_turn_seq
              AND player_turn_act.bet_faced_by_player_amount > 0 
        ),
        FoldedToTurnCBetActs AS (
            SELECT DISTINCT ftcbo.hand_db_id
            FROM FacedTurnCBetOpps ftcbo
            JOIN actions player_fold_act ON ftcbo.hand_db_id = player_fold_act.hand_db_id
            WHERE player_fold_act.player_id = ? AND player_fold_act.street = 'Turn' AND player_fold_act.action_type = 'folds'
        )
        SELECT (SELECT COUNT(*) FROM FacedTurnCBetOpps) as opps, (SELECT COUNT(*) FROM FoldedToTurnCBetActs) as acts
    """, (player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.fold_to_turn_cbet_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.fold_to_turn_cbet_actions = res['acts'] if res and res['acts'] is not None else 0
    
    # --- Donk Bet Turn ---
    # Oportunidade: NÃO houve Flop Aggressor OU jogador NÃO é o Flop Aggressor, FA ainda não agiu no turn,
    #                e ninguém betou antes do jogador no turn. (Mais simples: FA checkou flop, ou não houve FA)
    # Ação: Jogador beta no turn.
    cursor.execute("""
        WITH NoFlopAggressorOrFACheckedFlop AS (
            SELECT h.hand_db_id, h.flop_aggressor_id
            FROM hands h
            LEFT JOIN actions fa_flop_act ON h.hand_db_id = fa_flop_act.hand_db_id 
                                        AND fa_flop_act.player_id = h.flop_aggressor_id
                                        AND fa_flop_act.street = 'Flop'
                                        AND fa_flop_act.action_sequence = (SELECT MIN(fa_f_seq.action_sequence) FROM actions fa_f_seq WHERE fa_f_seq.hand_db_id = h.hand_db_id AND fa_f_seq.player_id = h.flop_aggressor_id AND fa_f_seq.street = 'Flop')
            WHERE h.flop_aggressor_id IS NULL 
               OR (h.flop_aggressor_id IS NOT NULL AND h.flop_aggressor_id != ? AND fa_flop_act.action_type = 'checks')
               OR (h.flop_aggressor_id = ? AND fa_flop_act.action_type = 'checks') -- Caso PFA seja o jogador e deu check flop
        ),
        DonkTurnOpps AS (
            SELECT DISTINCT nfa.hand_db_id
            FROM NoFlopAggressorOrFACheckedFlop nfa
            JOIN actions player_act_turn ON nfa.hand_db_id = player_act_turn.hand_db_id
            WHERE player_act_turn.player_id = ? AND player_act_turn.street = 'Turn'
              AND player_act_turn.action_type IN ('bets', 'checks')
              AND (nfa.flop_aggressor_id IS NULL OR player_act_turn.player_id != nfa.flop_aggressor_id) -- Jogador não é o FA (a menos que FA tenha checkado flop e agora done turn)
              AND NOT EXISTS ( -- Ninguém betou/raisou antes do jogador no turn
                  SELECT 1 FROM actions pre_donk_turn WHERE pre_donk_turn.hand_db_id = nfa.hand_db_id AND pre_donk_turn.street = 'Turn'
                    AND pre_donk_turn.action_type IN ('bets', 'raises') AND pre_donk_turn.action_sequence < player_act_turn.action_sequence
              )
              AND (nfa.flop_aggressor_id IS NULL OR NOT EXISTS ( -- Se houve FA, ele não agiu ainda no turn antes do donk
                  SELECT 1 FROM actions fa_turn_act WHERE fa_turn_act.hand_db_id = nfa.hand_db_id AND fa_turn_act.street = 'Turn'
                    AND fa_turn_act.player_id = nfa.flop_aggressor_id AND fa_turn_act.action_sequence < player_act_turn.action_sequence
              ))
        ),
        DonkTurnActs AS (
            SELECT DISTINCT dto.hand_db_id
            FROM DonkTurnOpps dto
            JOIN actions donk_b_turn ON dto.hand_db_id = donk_b_turn.hand_db_id
            WHERE donk_b_turn.player_id = ? AND donk_b_turn.street = 'Turn' AND donk_b_turn.action_type = 'bets'
        )
        SELECT (SELECT COUNT(*) FROM DonkTurnOpps) as opps, (SELECT COUNT(*) FROM DonkTurnActs) as acts
    """, (player_id, player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.donk_bet_turn_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.donk_bet_turn_actions = res['acts'] if res and res['acts'] is not None else 0
    
    # --- Fold to Donk Turn ---
    # Oportunidade: Jogador é FA (ou era PFA e não houve FA), e enfrenta um Donk Bet no Turn.
    # Ação: Jogador folda.
    # (Consulta similar a Fold to Donk Flop, mas adaptada para o Turn e FA)

    # --- Probe Bet Turn ---
    # Oportunidade: Não houve CBet Flop (PFA checkou ou não houve PFA/FA claro),
    #                ninguém betou no flop após o check do PFA (ou pote foi checkado),
    #                e é a vez do jogador no turn, ninguém betou antes no turn.
    # Ação: Jogador beta.
    # Esta é complexa. Precisa verificar se PFA checkou flop, e se o pote foi checkado até o jogador no turn.
    
    # --- Fold to Probe Turn ---
    # Oportunidade: Jogador (que poderia ser PFA que checkou flop) enfrenta um Probe Bet no Turn.
    # Ação: Jogador folda.

    # --- Bet vs Missed CBet Turn ---
    # Oportunidade: FA checkou turn (era opp de CBet Turn mas checkou), e é a vez do jogador.
    # Ação: Jogador beta.

    # --- Fold to Bet vs Missed CBet Turn ---
    # Oportunidade: Jogador é FA, checkou turn, outro betou, é a vez do FA.
    # Ação: FA folda.

    # --- Check-Raise Turn / Fold to XR Turn / etc. ---
    # Lógica similar às de Flop, mas para o Turn.

    # --- FTS Turn por Size --- (já coberto no stats_calculator.py principal)
    # --- Fold to Donk Turn por Size ---
    # (Similar ao de Flop, mas para Turn e usando a definição de Donk Turn)

    # --- Call-Fold Turn (Pagou Flop CBet/Bet, Foldou Turn CBet/Bet) por Size ---
    # Esta é específica: o jogador PRECISA ter pago uma aposta no flop,
    # e depois no turn enfrenta uma aposta e folda.
    cursor.execute("""
        WITH PlayerCalledFlopBet AS (
            SELECT DISTINCT a_flop_call.hand_db_id
            FROM actions a_flop_call
            WHERE a_flop_call.player_id = ? AND a_flop_call.street = 'Flop'
              AND a_flop_call.action_type = 'calls' AND a_flop_call.bet_faced_by_player_amount > 0
        ),
        FacedBetOnTurn AS (
            SELECT DISTINCT pcfb.hand_db_id,
                   CAST(ROUND((a_turn.bet_faced_by_player_amount * 100.0) / NULLIF(a_turn.pot_when_bet_was_made, 0)) AS INTEGER) as bet_perc_turn,
                   a_turn.action_type as turn_reaction
            FROM PlayerCalledFlopBet pcfb
            JOIN actions a_turn ON pcfb.hand_db_id = a_turn.hand_db_id
            WHERE a_turn.player_id = ? AND a_turn.street = 'Turn'
              AND a_turn.bet_faced_by_player_amount > 0 -- Enfrenta bet no turn
              AND a_turn.action_type IN ('calls', 'folds', 'raises') -- Teve uma reação ao bet
        )
        SELECT bet_perc_turn, turn_reaction, COUNT(*) as count
        FROM FacedBetOnTurn
        GROUP BY bet_perc_turn, turn_reaction
    """, (player_id, player_id))

    for row in cursor.fetchall():
        sg = ps.get_bet_size_group(row['bet_perc_turn'] if row['bet_perc_turn'] is not None else None)
        if sg != "N/A":
            ps.call_fold_turn_opportunities_by_size[sg] += row['count']
            if row['turn_reaction'] == 'folds':
                ps.call_fold_turn_actions_by_size[sg] += row['count']