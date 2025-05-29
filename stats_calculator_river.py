# stats_calculator_river.py
import sqlite3
from collections import defaultdict
# from .stats_calculator import PlayerStats, _get_simplified_hand_category_from_description, FOLD_CLASS_THRESHOLDS, BLUFF_CLASS_THRESHOLDS, _classify_percentage
# Se PlayerStats e outras constantes/funções estiverem no stats_calculator.py principal

def calculate_river_stats_for_player(ps, cursor: sqlite3.Cursor, player_id: int):
    """
    Calcula e preenche as estatísticas de River para o objeto PlayerStats (ps).
    """
    if ps.hands_played == 0: return

    # --- CBet River ---
    # Oportunidade: Jogador foi Turn Aggressor (TA), chegou no river, ninguém betou antes dele no river.
    # Ação: Jogador beta no river.
    cursor.execute("""
        WITH TurnAggressorIsPlayer AS (
            SELECT hand_db_id FROM hands WHERE turn_aggressor_id = ?
        ),
        CBetRiverOpps AS (
            SELECT DISTINCT taip.hand_db_id
            FROM TurnAggressorIsPlayer taip
            JOIN actions ra ON taip.hand_db_id = ra.hand_db_id
            WHERE ra.player_id = ? AND ra.street = 'River'
              AND ra.action_type IN ('bets', 'checks')
              AND NOT EXISTS (SELECT 1 FROM actions pre_river_bet 
                              WHERE pre_river_bet.hand_db_id = taip.hand_db_id AND pre_river_bet.street = 'River'
                              AND pre_river_bet.action_type IN ('bets', 'raises') 
                              AND pre_river_bet.action_sequence < ra.action_sequence)
        ),
        CBetRiverActs AS (
            SELECT DISTINCT cbo_r.hand_db_id
            FROM CBetRiverOpps cbo_r
            JOIN actions ra_bet ON cbo_r.hand_db_id = ra_bet.hand_db_id
            WHERE ra_bet.player_id = ? AND ra_bet.street = 'River' AND ra_bet.action_type = 'bets'
            AND ra_bet.action_sequence = (SELECT MIN(act_seq.action_sequence) FROM actions act_seq 
                                          WHERE act_seq.hand_db_id = cbo_r.hand_db_id AND act_seq.street = 'River' 
                                          AND act_seq.player_id = ? AND act_seq.action_type IN ('bets', 'checks'))
        )
        SELECT (SELECT COUNT(*) FROM CBetRiverOpps) as opps, (SELECT COUNT(*) FROM CBetRiverActs) as acts
    """, (player_id, player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.cbet_river_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.cbet_river_actions = res['acts'] if res and res['acts'] is not None else 0
    
    # --- Fold to River CBet ---
    # (Lógica similar a Fold to Turn CBet, usando turn_aggressor_id)

    # --- Bet River --- (Qualquer bet no river quando é a vez do jogador e não há aposta para pagar)
    cursor.execute("""
        SELECT COUNT(DISTINCT a.hand_db_id)
        FROM actions a
        WHERE a.player_id = ? AND a.street = 'River' AND a.action_type IN ('bets', 'checks')
          AND a.amount_to_call_for_player = 0 -- Não enfrenta aposta
    """, (player_id,))
    res_opp = cursor.fetchone()
    ps.bet_river_opportunities = res_opp[0] if res_opp and res_opp[0] is not None else 0
    
    cursor.execute("""
        SELECT COUNT(DISTINCT a.hand_db_id)
        FROM actions a
        WHERE a.player_id = ? AND a.street = 'River' AND a.action_type = 'bets'
          AND a.amount_to_call_for_player = 0
    """, (player_id,))
    res_act = cursor.fetchone()
    ps.bet_river_actions = res_act[0] if res_act and res_act[0] is not None else 0

    # --- Donk Bet River / Fold to Donk River ---
    # --- Probe Bet River / Fold to Probe River ---
    # --- Bet vs Missed CBet River / Fold to Bet vs Missed CBet River ---
    # --- Check-Raise River / Fold to XR River ---
    # (Lógica similar às de Turn, adaptando o agressor da street anterior)

    # --- FTS River por Size --- (Já coberto no stats_calculator.py principal)
    # --- Fold to Donk River por Size ---

    # --- FTS River por Linha (BBB, BXB, XBB, XXB) e Size ---
    # PFA é o jogador, linha é definida por suas ações Flop (B/X), Turn (B/X), River (B)
    # E oponente (outro jogador) folda para o bet do PFA no river.
    # Esta é uma consulta complexa que precisa identificar a linha do PFA.
    # (A consulta de composição de river já faz parte disso, pode ser adaptada)

    # --- Composição de River por Linha, Size e Mão (PFA é o jogador) ---
    # (A consulta já está no stats_calculator.py principal, pode ser chamada ou movida/adaptada aqui)

    # --- CCF vs Triple Barrel ---
    # Oportunidade: Jogador deu C/C Flop, C/C Turn, e enfrenta 3rd barrel do PFA no River.
    # Ação: Jogador folda.
    cursor.execute("""
        WITH PFAIsNOTPlayer AS (SELECT hand_db_id, preflop_aggressor_id as pfa_id FROM hands WHERE preflop_aggressor_id IS NOT NULL AND preflop_aggressor_id != ?),
        PFATripleBarrelHands AS ( -- Mãos onde PFA betou F, T, R
            SELECT DISTINCT pnp.hand_db_id, pnp.pfa_id
            FROM PFAIsNOTPlayer pnp
            WHERE EXISTS (SELECT 1 FROM actions fa WHERE fa.hand_db_id = pnp.hand_db_id AND fa.player_id = pnp.pfa_id AND fa.street = 'Flop' AND fa.action_type = 'bets')
              AND EXISTS (SELECT 1 FROM actions ta WHERE ta.hand_db_id = pnp.hand_db_id AND ta.player_id = pnp.pfa_id AND ta.street = 'Turn' AND ta.action_type = 'bets')
              AND EXISTS (SELECT 1 FROM actions ra WHERE ra.hand_db_id = pnp.hand_db_id AND ra.player_id = pnp.pfa_id AND ra.street = 'River' AND ra.action_type = 'bets')
        ),
        PlayerCalledFlopAndTurn AS (
            SELECT DISTINCT ptbh.hand_db_id
            FROM PFATripleBarrelHands ptbh
            WHERE 
                EXISTS (SELECT 1 FROM actions fc WHERE fc.hand_db_id = ptbh.hand_db_id AND fc.player_id = ? AND fc.street = 'Flop' AND fc.action_type = 'calls' AND fc.bet_faced_by_player_amount > 0)
            AND EXISTS (SELECT 1 FROM actions tc WHERE tc.hand_db_id = ptbh.hand_db_id AND tc.player_id = ? AND tc.street = 'Turn' AND tc.action_type = 'calls' AND tc.bet_faced_by_player_amount > 0)
        ),
        CCFvsTBOpps AS ( -- Jogador (que deu C/C F,T) enfrenta 3rd barrel
            SELECT DISTINCT pcft.hand_db_id
            FROM PlayerCalledFlopAndTurn pcft
            JOIN actions river_pfa_bet ON pcft.hand_db_id = river_pfa_bet.hand_db_id
            JOIN actions player_river_act ON pcft.hand_db_id = player_river_act.hand_db_id
            JOIN PFATripleBarrelHands ptbh_check ON pcft.hand_db_id = ptbh_check.hand_db_id -- Para pegar pfa_id
            WHERE river_pfa_bet.player_id = ptbh_check.pfa_id AND river_pfa_bet.street = 'River' AND river_pfa_bet.action_type = 'bets'
              AND player_river_act.player_id = ? AND player_river_act.street = 'River'
              AND player_river_act.action_sequence > river_pfa_bet.action_sequence
              AND player_river_act.bet_faced_by_player_amount > 0
        ),
        CCFvsTBActs AS (
            SELECT DISTINCT opps.hand_db_id
            FROM CCFvsTBOpps opps
            JOIN actions player_fold ON opps.hand_db_id = player_fold.hand_db_id
            WHERE player_fold.player_id = ? AND player_fold.street = 'River' AND player_fold.action_type = 'folds'
        )
        SELECT (SELECT COUNT(*) FROM CCFvsTBOpps) as opps, (SELECT COUNT(*) FROM CCFvsTBActs) as acts
    """, (player_id, player_id, player_id, player_id, player_id)) # player_id usado várias vezes
    res = cursor.fetchone()
    ps.ccf_triple_barrel_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.ccf_triple_barrel_actions = res['acts'] if res and res['acts'] is not None else 0

    # --- BBF vs Donk River ---
    # Oportunidade: Jogador betou flop, betou turn (foi o agressor F & T), e enfrenta um Donk Bet no River.
    # Ação: Jogador folda.
    cursor.execute("""
        WITH PlayerBetFlopAndTurn AS (
            SELECT DISTINCT fa.hand_db_id
            FROM actions fa
            JOIN actions ta ON fa.hand_db_id = ta.hand_db_id AND fa.player_id = ta.player_id
            WHERE fa.player_id = ? AND fa.street = 'Flop' AND fa.action_type = 'bets'
              AND ta.street = 'Turn' AND ta.action_type = 'bets'
              -- Adicionar condições para garantir que foram CBets ou bets agressivas, não donks do próprio jogador
        ),
        FacedRiverDonk AS (
            SELECT DISTINCT pbft.hand_db_id, river_donk.action_sequence as river_donk_seq
            FROM PlayerBetFlopAndTurn pbft
            JOIN actions river_donk ON pbft.hand_db_id = river_donk.hand_db_id
            WHERE river_donk.street = 'River' AND river_donk.action_type = 'bets'
              AND river_donk.player_id != ? -- Donk por outro jogador
              AND NOT EXISTS ( -- Jogador (agressor F,T) não agiu ainda no river antes do donk
                  SELECT 1 FROM actions player_prev_river_act
                  WHERE player_prev_river_act.hand_db_id = pbft.hand_db_id AND player_prev_river_act.street = 'River'
                    AND player_prev_river_act.player_id = ? AND player_prev_river_act.action_sequence < river_donk.action_sequence
              )
        ),
        BBFoldedToRiverDonk AS (
            SELECT DISTINCT frd.hand_db_id
            FROM FacedRiverDonk frd
            JOIN actions player_fold_river ON frd.hand_db_id = player_fold_river.hand_db_id
            WHERE player_fold_river.player_id = ? AND player_fold_river.street = 'River' AND player_fold_river.action_type = 'folds'
            AND player_fold_river.action_sequence > frd.river_donk_seq
        )
        SELECT (SELECT COUNT(*) FROM FacedRiverDonk) as opps, (SELECT COUNT(*) FROM BBFoldedToRiverDonk) as acts
    """, (player_id, player_id, player_id, player_id))
    res = cursor.fetchone()
    ps.bbf_vs_donk_river_opportunities = res['opps'] if res and res['opps'] is not None else 0
    ps.bbf_vs_donk_river_actions = res['acts'] if res and res['acts'] is not None else 0