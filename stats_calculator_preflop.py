import sqlite3
from typing import Optional

class PreflopStats:
    def __init__(self):
        self.vpip_actions = 0
        self.vpip_opportunities = 0
        self.pfr_actions = 0
        self.pfr_opportunities = 0
        self.threebet_actions = 0
        self.threebet_opportunities = 0
        self.fold_to_threebet_actions = 0
        self.fold_to_threebet_opportunities = 0


def _count(query: str, cursor: sqlite3.Cursor, params: tuple) -> int:
    cursor.execute(query, params)
    row = cursor.fetchone()
    if not row:
        return 0
    value = row[0]
    return value if value else 0


def calculate_preflop_stats_for_player(ps, cursor: sqlite3.Cursor, player_id: int) -> Optional[PreflopStats]:
    """Calcula estatísticas de pré-flop para o jogador indicado.

    O objeto ``ps`` é atualizado in-place com os valores calculados. A função
    também retorna o objeto ``PreflopStats`` resumido para uso externo, se
    necessário.
    """
    stats = PreflopStats()

    # Total de mãos jogadas
    stats.vpip_opportunities = _count(
        "SELECT COUNT(DISTINCT hand_db_id) FROM hand_players WHERE player_id=?",
        cursor,
        (player_id,)
    )

    stats.pfr_opportunities = stats.vpip_opportunities
    stats.threebet_opportunities = 0
    stats.fold_to_threebet_opportunities = 0

    if stats.vpip_opportunities == 0:
        # Atualiza objeto PlayerStats com valores zerados
        ps.vpip_opportunities = 0
        ps.vpip_actions = 0
        ps.pfr_opportunities = 0
        ps.pfr_actions = 0
        ps.threebet_opportunities = 0
        ps.threebet_actions = 0
        ps.fold_to_pf_3bet_opportunities = 0
        ps.fold_to_pf_3bet_actions = 0
        return stats

    # VPIP
    stats.vpip_actions = _count(
        """
        SELECT COUNT(DISTINCT a.hand_db_id)
        FROM actions a
        JOIN hand_players hp ON a.hand_db_id = hp.hand_db_id AND a.player_id = hp.player_id
        JOIN hands h ON a.hand_db_id = h.hand_db_id
        WHERE a.player_id=? AND a.street='Preflop'
          AND a.action_type IN ('calls','bets','raises')
          AND NOT (
            (hp.position='SB' AND a.action_type='calls' AND a.amount=h.big_blind_amount/2) OR
            (hp.position='BB' AND a.action_type='calls' AND a.amount=0)
          )
        """,
        cursor,
        (player_id,),
    )

    # PFR
    stats.pfr_actions = _count(
        "SELECT COUNT(DISTINCT hand_db_id) FROM actions WHERE player_id=? AND street='Preflop' AND action_type IN ('bets','raises')",
        cursor,
        (player_id,),
    )

    # 3bet e Fold to 3bet
    cursor.execute(
        """
        SELECT hand_db_id, player_id, action_type, action_sequence
        FROM actions
        WHERE street='Preflop'
        ORDER BY hand_db_id, action_sequence
        """
    )
    rows = cursor.fetchall()
    hands = {}
    for row in rows:
        hand_id = row[0]
        if hand_id not in hands:
            hands[hand_id] = []
        hands[hand_id].append(row)

    for hand_id, actions in hands.items():
        first_raise = None
        second_raise = None
        player_action_index = None
        for idx, act in enumerate(actions):
            pid = act[1]
            a_type = act[2]
            if a_type in ("bets", "raises"):
                if not first_raise:
                    first_raise = pid
                elif not second_raise:
                    second_raise = pid
            if pid == player_id and player_action_index is None:
                player_action_index = idx

        if player_action_index is None:
            continue

        # 3bet opportunity: there is exactly one raise before player's action
        pre_actions = [a for a in actions if a[3] < actions[player_action_index][3] and a[2] in ('bets', 'raises')]
        if len(pre_actions) == 1 and pre_actions[0][1] != player_id:
            stats.threebet_opportunities += 1
            if actions[player_action_index][2] in ('bets', 'raises'):
                stats.threebet_actions += 1
        # Fold to 3bet opportunity
        # If player is the first raiser and another player reraises and player later folds
        if first_raise == player_id and second_raise and second_raise != player_id:
            stats.fold_to_threebet_opportunities += 1
            for act in actions[player_action_index + 1 : ]:
                if act[1] == player_id and act[2] == 'folds':
                    stats.fold_to_threebet_actions += 1
                    break

    # Propaga resultados para o objeto PlayerStats
    ps.vpip_opportunities = stats.vpip_opportunities
    ps.vpip_actions = stats.vpip_actions
    ps.pfr_opportunities = stats.pfr_opportunities
    ps.pfr_actions = stats.pfr_actions
    ps.three_bet_pf_opportunities = stats.threebet_opportunities
    ps.three_bet_pf_actions = stats.threebet_actions
    ps.fold_to_pf_3bet_opportunities = stats.fold_to_threebet_opportunities
    ps.fold_to_pf_3bet_actions = stats.fold_to_threebet_actions

    return stats
