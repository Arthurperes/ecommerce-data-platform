"""
Matriz de decisão comercial
Projeto E-commerce Data Platform

Combina:
1. Perfil comportamental do cliente
2. Score de propensão à conversão

Score:
0.00 a 0.25 -> Baixa propensão
0.25 a 0.75 -> Moderada propensão
0.75 a 1.00 -> Alta propensão
"""


PROFILE_NAMES = {
    0: "Navegador Indeciso",
    1: "Comprador de Alta Intencao",
    2: "Cacador de Descontos"
}


def get_score_band(score):

    if score < 0 or score > 1:
        raise ValueError(
            "O conversion_score deve estar entre 0 e 1."
        )

    if score <= 0.25:
        return "Baixa Propensao"

    elif score <= 0.75:
        return "Moderada Propensao"

    else:
        return "Alta Propensao"


def get_action(profile, score):

    score_band = get_score_band(score)

    # =========================================================
    # 1. COMPRADOR DE ALTA INTENCAO
    # =========================================================

    if profile == "Comprador de Alta Intencao":

        if score_band == "Baixa Propensao":

            action = "Notificacao Simples"

            benefit = (
                "Lembrete de carrinho sem desconto"
            )

            message = (
                "Seus produtos ainda estao te esperando!"
            )

        elif score_band == "Moderada Propensao":

            action = "Notificacao de Escassez"

            benefit = (
                "Urgencia sem concessao de desconto"
            )

            message = (
                "Restam poucas unidades dos produtos "
                "do seu carrinho."
            )

        else:

            action = "Frete Gratis"

            benefit = (
                "Isencao do valor de entrega"
            )

            message = (
                "Finalize sua compra com frete gratis."
            )

    # =========================================================
    # 2. NAVEGADOR INDECISO
    # =========================================================

    elif profile == "Navegador Indeciso":

        if score_band == "Baixa Propensao":

            action = "Prova Social"

            benefit = (
                "Reforco de confianca sem desconto"
            )

            message = (
                "Esse produto esta entre os mais vendidos."
            )

        elif score_band == "Moderada Propensao":

            action = "Frete Gratis"

            benefit = (
                "Reducao da barreira de compra"
            )

            message = (
                "Que tal finalizar agora com frete gratis?"
            )

        else:

            action = "Frete Gratis + 5% OFF"

            benefit = (
                "Incentivo combinado para fechamento"
            )

            message = (
                "Finalize agora com frete gratis "
                "e 5% de desconto."
            )

    # =========================================================
    # 3. CACADOR DE DESCONTOS
    # =========================================================

    elif profile == "Cacador de Descontos":

        if score_band == "Baixa Propensao":

            action = "Moedas / Pontos"

            benefit = (
                "Beneficio de baixo custo promocional"
            )

            message = (
                "Ganhe moedas ao finalizar sua compra."
            )

        elif score_band == "Moderada Propensao":

            action = "Cupom 5% OFF ou Frete"

            benefit = (
                "Desconto promocional leve"
            )

            message = (
                "Voce ganhou uma condicao especial "
                "para finalizar seu carrinho."
            )

        else:

            action = "Frete Gratis + 10% a 15% OFF"

            benefit = (
                "Incentivo comercial de maior intensidade"
            )

            message = (
                "Oferta especial para voce finalizar "
                "sua compra agora."
            )

    else:

        raise ValueError(
            f"Perfil desconhecido: {profile}"
        )

    return {
        "profile": profile,
        "conversion_score": round(
            float(score),
            4
        ),
        "score_band": score_band,
        "action": action,
        "benefit": benefit,
        "message": message
    }


def get_action_from_cluster(
    cluster,
    score
):

    if cluster not in PROFILE_NAMES:

        raise ValueError(
            f"Cluster desconhecido: {cluster}"
        )

    profile = PROFILE_NAMES[
        cluster
    ]

    return get_action(
        profile,
        score
    )


def print_decision(
    cluster,
    score
):

    decision = get_action_from_cluster(
        cluster,
        score
    )

    print(
        "========================================"
    )

    print(
        f"Perfil: {decision['profile']}"
    )

    print(
        f"Score: {decision['conversion_score']:.2%}"
    )

    print(
        f"Faixa: {decision['score_band']}"
    )

    print(
        f"Acao: {decision['action']}"
    )

    print(
        f"Beneficio: {decision['benefit']}"
    )

    print(
        f"Mensagem: {decision['message']}"
    )

    print(
        "========================================"
    )


def main():

    print(
        "\n=== TESTE DA MATRIZ DE DECISAO ===\n"
    )

    # Navegador Indeciso
    print_decision(
        cluster=0,
        score=0.20
    )

    print_decision(
        cluster=0,
        score=0.55
    )

    print_decision(
        cluster=0,
        score=0.85
    )

    # Comprador de Alta Intencao
    print_decision(
        cluster=1,
        score=0.20
    )

    print_decision(
        cluster=1,
        score=0.55
    )

    print_decision(
        cluster=1,
        score=0.85
    )

    # Cacador de Descontos
    print_decision(
        cluster=2,
        score=0.20
    )

    print_decision(
        cluster=2,
        score=0.55
    )

    print_decision(
        cluster=2,
        score=0.85
    )

    print(
        "\nMATRIZ DE DECISAO VALIDADA COM SUCESSO"
    )


if __name__ == "__main__":
    main()