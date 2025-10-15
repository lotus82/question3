from typing import Optional, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_categories_df: DataFrame,
    product_id_col: str = "id",
    product_name_col: str = "name",
    category_id_col: str = "id",
    category_name_col: str = "name",
    pc_product_id_col: str = "product_id",
    pc_category_id_col: str = "category_id",
    output_product_id_col: Optional[str] = None,
    output_category_id_col: Optional[str] = None,
    output_product_name_col: str = "product_name",
    output_category_name_col: str = "category_name",
    order_by: Optional[List[Tuple[str, str]]] = None,
) -> DataFrame:


    p = products_df.select(
        F.col(product_id_col).alias("_p_id"),      # внутренний id продукта
        F.col(product_name_col).alias("_p_name"),  # внутреннее имя продукта
    )

    c = categories_df.select(
        F.col(category_id_col).alias("_c_id"),      # внутренний id категории
        F.col(category_name_col).alias("_c_name"),  # внутреннее имя категории
    )

    pc = product_categories_df.select(
        F.col(pc_product_id_col).alias("_pc_p_id"),  # id продукта в связях
        F.col(pc_category_id_col).alias("_pc_c_id"), # id категории в связях
    )

    joined = (
        p.join(pc, p["_p_id"] == pc["_pc_p_id"], how="left")
         .join(c, pc["_pc_c_id"] == c["_c_id"], how="left")
    )

    output_cols = []
    if output_product_id_col is not None:
        output_cols.append(F.col("_p_id").alias(output_product_id_col))
    output_cols.append(F.col("_p_name").alias(output_product_name_col))
    if output_category_id_col is not None:
        output_cols.append(F.col("_c_id").alias(output_category_id_col))
    output_cols.append(F.col("_c_name").alias(output_category_name_col))

    result = joined.select(*output_cols).distinct()

    if order_by:
        sort_cols = []
        for col_name, direction in order_by:
            if direction.lower() == "desc":
                sort_cols.append(F.col(col_name).desc())
            else:
                sort_cols.append(F.col(col_name).asc())
        result = result.orderBy(*sort_cols)

    return result
