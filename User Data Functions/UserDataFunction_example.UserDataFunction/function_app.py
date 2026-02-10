import datetime
import fabric.functions as fn
import logging

udf = fn.UserDataFunctions()


import pandas as pd 

# This function standardizes inconsistent product category names from different data sources

@udf.function()
def standardize_category(productName: str, rawCategory: str) -> dict:
    # Define category mappings for common variations
    category_mapping = {
        "electronics": ["electronic", "electronics", "tech", "devices"],
        "clothing": ["clothes", "clothing", "apparel", "fashion"],
        "home_goods": ["home", "household", "home goods", "furniture"],
        "food": ["food", "grocery", "groceries", "snacks"],
        "books": ["book", "books", "reading", "literature"]
    }
    
    # Normalize the input
    raw_lower = rawCategory.lower().strip()
    
    # Find the standardized category
    standardized = "other"
    for standard_name, variations in category_mapping.items():
        if raw_lower in variations:
            standardized = standard_name
            break
    
    return {
        "product_name": productName,
        "original_category": rawCategory,
        "standardized_category": standardized,
        "needs_review": standardized == "other"
    }