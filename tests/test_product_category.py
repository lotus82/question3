import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

from product_category import build_product_category_pairs


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("product-category-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def assert_df_equals(actual_df, expected_rows, select_cols, sort_by):
    actual = [tuple(r) for r in actual_df.select(*select_cols).orderBy(*sort_by).collect()]
    expected = sorted(expected_rows, key=lambda r: tuple(r))
    assert actual == expected


def test_many_to_many_and_orphans(spark: SparkSession):
    products = spark.createDataFrame(
        [
            Row(id=1, name="ProdA"),
            Row(id=2, name="ProdB"),
            Row(id=3, name="ProdC"),  # без категорий
        ]
    )

    categories = spark.createDataFrame(
        [
            Row(id=10, name="CatX"),
            Row(id=11, name="CatY"),
            Row(id=12, name="CatZ"),
        ]
    )

    product_categories = spark.createDataFrame(
        [
            Row(product_id=1, category_id=10),
            Row(product_id=1, category_id=11),
            Row(product_id=2, category_id=11),
        ]
    )

    result = build_product_category_pairs(
        products,
        categories,
        product_categories,
        output_product_id_col="product_id",
        output_category_id_col="category_id",
        output_product_name_col="product",
        output_category_name_col="category",
        order_by=[("product", "asc"), ("category", "asc")],
    )

    expected = [
        (1, "ProdA", 10, "CatX"),
        (1, "ProdA", 11, "CatY"),
        (2, "ProdB", 11, "CatY"),
        (3, "ProdC", None, None),  # продукт без категорий
    ]
    assert_df_equals(
        result,
        expected,
        select_cols=["product_id", "product", "category_id", "category"],
        sort_by=["product", "category"],
    )


def test_categories_without_products(spark: SparkSession):
    products = spark.createDataFrame([Row(id=1, name="P1")])
    categories = spark.createDataFrame([Row(id=10, name="C1"), Row(id=11, name="C2")])
    product_categories = spark.createDataFrame([Row(product_id=1, category_id=10)])

    result = build_product_category_pairs(
        products,
        categories,
        product_categories,
        output_product_name_col="product",
        output_category_name_col="category",
        order_by=[("product", "asc"), ("category", "asc")],
    )

    expected = [
        ("P1", "C1"),
    ]
    
    assert_df_equals(
        result,
        expected,
        select_cols=["product", "category"],
        sort_by=["product", "category"],
    )


def test_duplicates_in_links_collapsed_by_distinct(spark: SparkSession):
    products = spark.createDataFrame([Row(id=1, name="P1")])
    categories = spark.createDataFrame([Row(id=10, name="C1")])
    product_categories = spark.createDataFrame([
        Row(product_id=1, category_id=10),
        Row(product_id=1, category_id=10),  
    ])

    result = build_product_category_pairs(
        products,
        categories,
        product_categories,
        output_product_name_col="product",
        output_category_name_col="category",
        order_by=[("product", "asc"), ("category", "asc")],
    )

    expected = [
        ("P1", "C1"),
    ]
    assert_df_equals(
        result,
        expected,
        select_cols=["product", "category"],
        sort_by=["product", "category"],
    )


def test_custom_column_names(spark: SparkSession):
    products = spark.createDataFrame([Row(pid=1, pname="PX"), Row(pid=2, pname="PY")])
    categories = spark.createDataFrame([Row(cid=10, cname="CX")])
    links = spark.createDataFrame([Row(p=1, c=10)])

    result = build_product_category_pairs(
        products,
        categories,
        links,
        product_id_col="pid",
        product_name_col="pname",
        category_id_col="cid",
        category_name_col="cname",
        pc_product_id_col="p",
        pc_category_id_col="c",
        output_product_id_col="p_id",
        output_category_id_col="c_id",
        output_product_name_col="p_name",
        output_category_name_col="c_name",
        order_by=[("p_name", "asc"), ("c_name", "asc")],
    )

    expected = [
        (1, "PX", 10, "CX"),
        (2, "PY", None, None),
    ]
    assert_df_equals(
        result,
        expected,
        select_cols=["p_id", "p_name", "c_id", "c_name"],
        sort_by=["p_name", "c_name"],
    )


