# test_for_hh

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def get_product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_category_links_df: DataFrame
) -> DataFrame:
    """ 
    products_df - датафрейм продуктов с колонками: product_id, product_name
    categories_df - датафрейм категорий с колонками: category_id, category_name
    product_category_links_df - датафрейм связей с колонками: product_id, category_id
    """
    
    products_with_categories = (
        products_df.join(
            product_category_links_df,
            on="product_id",
            how="left"
        )
        .join(
            categories_df,
            on="category_id",
            how="left"
        )
        .select(
            col("product_name"),
            col("category_name")
        )
    )
    
    return products_with_categories
