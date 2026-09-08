import pytest

from src.decision_matrix import (
    get_action,
    get_action_from_cluster,
    get_score_band
)


def test_score_band_low():
    assert get_score_band(0.00) == "Baixa Propensao"
    assert get_score_band(0.25) == "Baixa Propensao"


def test_score_band_medium():
    assert get_score_band(0.26) == "Moderada Propensao"
    assert get_score_band(0.75) == "Moderada Propensao"


def test_score_band_high():
    assert get_score_band(0.76) == "Alta Propensao"
    assert get_score_band(1.00) == "Alta Propensao"


def test_invalid_score_below_zero():
    with pytest.raises(ValueError):
        get_score_band(-0.01)


def test_invalid_score_above_one():
    with pytest.raises(ValueError):
        get_score_band(1.01)


def test_high_intent_low_score():
    result = get_action(
        "Comprador de Alta Intencao",
        0.20
    )

    assert result["score_band"] == "Baixa Propensao"
    assert result["action"] == "Notificacao Simples"


def test_high_intent_medium_score():
    result = get_action(
        "Comprador de Alta Intencao",
        0.55
    )

    assert result["score_band"] == "Moderada Propensao"
    assert result["action"] == "Notificacao de Escassez"


def test_high_intent_high_score():
    result = get_action(
        "Comprador de Alta Intencao",
        0.85
    )

    assert result["score_band"] == "Alta Propensao"
    assert result["action"] == "Frete Gratis"


def test_browser_low_score():
    result = get_action(
        "Navegador Indeciso",
        0.20
    )

    assert result["score_band"] == "Baixa Propensao"
    assert result["action"] == "Prova Social"


def test_browser_medium_score():
    result = get_action(
        "Navegador Indeciso",
        0.55
    )

    assert result["score_band"] == "Moderada Propensao"
    assert result["action"] == "Frete Gratis"


def test_browser_high_score():
    result = get_action(
        "Navegador Indeciso",
        0.85
    )

    assert result["score_band"] == "Alta Propensao"
    assert result["action"] == "Frete Gratis + 5% OFF"


def test_bargain_hunter_low_score():
    result = get_action(
        "Cacador de Descontos",
        0.20
    )

    assert result["score_band"] == "Baixa Propensao"
    assert result["action"] == "Moedas / Pontos"


def test_bargain_hunter_medium_score():
    result = get_action(
        "Cacador de Descontos",
        0.55
    )

    assert result["score_band"] == "Moderada Propensao"
    assert result["action"] == "Cupom 5% OFF ou Frete"


def test_bargain_hunter_high_score():
    result = get_action(
        "Cacador de Descontos",
        0.85
    )

    assert result["score_band"] == "Alta Propensao"
    assert result["action"] == "Frete Gratis + 10% a 15% OFF"


def test_cluster_zero_mapping():
    result = get_action_from_cluster(
        0,
        0.55
    )

    assert result["profile"] == "Navegador Indeciso"


def test_cluster_one_mapping():
    result = get_action_from_cluster(
        1,
        0.55
    )

    assert result["profile"] == "Comprador de Alta Intencao"


def test_cluster_two_mapping():
    result = get_action_from_cluster(
        2,
        0.55
    )

    assert result["profile"] == "Cacador de Descontos"


def test_invalid_profile():
    with pytest.raises(ValueError):
        get_action(
            "Perfil Inexistente",
            0.50
        )


def test_invalid_cluster():
    with pytest.raises(ValueError):
        get_action_from_cluster(
            99,
            0.50
        )