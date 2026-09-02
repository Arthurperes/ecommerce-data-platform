"""
Modulo: feature_engineering.py
Descrição: Processamento e Engenharia de Features (Camadas Silver -> Gold).
Converte logs de eventos de e-commerce (view, cart, purchase) em um dataset de 
carrinhos agregados com features preditivas e o rótulo alvo (target_is_abandoned).
"""

import pandas as pd
import numpy as np

def build_cart_features(df_events):
    """
    Agrega o log de eventos brutos ao nível de sessão/carrinho (user_session).
    
    Parâmetros:
        df_events (pd.DataFrame): DataFrame com colunas [event_time, event_type, product_id,
                                                    category_id, category_code, brand, price,
                                                    user_id, user_session]
                                                    
    Retorna:
        pd.DataFrame: Gold Dataset com 1 linha por carrinho com itens (`user_session`), 
                      contendo features numéricas/categóricas e a coluna target `is_abandoned`.
    """
    # Converte event_time para datetime
    df_events["event_time_dt"] = pd.to_datetime(df_events["event_time"])
    
    # Filtra apenas sessões que tiveram pelo menos 1 evento de carrinho ('cart')
    sessions_with_cart = df_events[df_events["event_type"] == "cart"]["user_session"].unique()
    df_cart_sessions = df_events[df_events["user_session"].isin(sessions_with_cart)].copy()
    
    cart_records = []
    
    grouped = df_cart_sessions.groupby("user_session")
    
    for session_id, group in grouped:
        user_id = group["user_id"].iloc[0]
        
        # Separar por tipo de evento
        view_events = group[group["event_type"] == "view"]
        cart_events = group[group["event_type"] == "cart"]
        purchase_events = group[group["event_type"] == "purchase"]
        
        # Target: Se NÃO houve compra registrada na mesma sessão para o carrinho -> Abandono (1), senão (0)
        is_abandoned = 1 if purchase_events.empty else 0
        
        # Features do Carrinho
        num_cart_items = len(cart_events)
        total_cart_value = cart_events["price"].sum()
        max_item_price = cart_events["price"].max()
        min_item_price = cart_events["price"].min()
        avg_item_price = cart_events["price"].mean()
        
        # Diversidade de marcas e categorias no carrinho
        num_distinct_brands = cart_events["brand"].nunique()
        num_distinct_categories = cart_events["category_code"].nunique()
        
        # Categoria principal no carrinho (tratando valores nulos com segurança)
        cat_non_null = cart_events["category_code"].dropna()
        if not cat_non_null.empty:
            mode_series = cat_non_null.mode()
            main_category = mode_series.iloc[0] if not mode_series.empty else "other"
        else:
            main_category = "other"
            
        main_category_group = str(main_category).split(".")[0] if "." in str(main_category) else str(main_category)
        
        # Features de Engajamento e Navegação
        num_views_before_cart = len(view_events)
        view_to_cart_ratio = num_views_before_cart / max(1, num_cart_items)
        
        # Tempo total da sessão
        first_event_time = group["event_time_dt"].min()
        last_cart_time = cart_events["event_time_dt"].max()
        session_duration_sec = (last_cart_time - first_event_time).total_seconds()
        
        # Temporal
        hour_of_day = last_cart_time.hour
        day_of_week = last_cart_time.dayofweek
        is_weekend = 1 if day_of_week in [5, 6] else 0
        is_night = 1 if hour_of_day in [0, 1, 2, 3, 4, 5, 22, 23] else 0
        
        cart_records.append({
            "user_session": session_id,
            "user_id": user_id,
            "num_cart_items": num_cart_items,
            "total_cart_value": total_cart_value,
            "max_item_price": max_item_price,
            "min_item_price": min_item_price,
            "avg_item_price": avg_item_price,
            "num_distinct_brands": num_distinct_brands,
            "num_distinct_categories": num_distinct_categories,
            "main_category": main_category_group,
            "num_views_before_cart": num_views_before_cart,
            "view_to_cart_ratio": view_to_cart_ratio,
            "session_duration_sec": max(0.0, session_duration_sec),
            "hour_of_day": hour_of_day,
            "day_of_week": day_of_week,
            "is_weekend": is_weekend,
            "is_night": is_night,
            "is_abandoned": is_abandoned
        })
        
    df_gold = pd.DataFrame(cart_records)
    return df_gold

if __name__ == "__main__":
    from data_generator import generate_ecommerce_events
    df_raw = generate_ecommerce_events(num_sessions=500)
    df_gold = build_cart_features(df_raw)
    print(f"Dataset Gold construído com sucesso! Total de carrinhos: {len(df_gold)}")
    print(f"Taxa de abandono observada: {df_gold['is_abandoned'].mean():.2%}")
    print(df_gold.head())
