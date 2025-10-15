# PySpark: Продукты и категории

Вопрос №3
В PySpark приложении датафреймами (pyspark.sql.DataFrame) заданы продукты, категории и их связи. Каждому продукту может соответствовать несколько категорий или ни одной. А каждой категории может соответствовать несколько продуктов или ни одного. Напишите метод на PySpark, который в одном датафрейме вернет все пары «Имя продукта – Имя категории» и имена всех продуктов, у которых нет категорий. Напишите тесты на ваш метод.

Решение содержит функцию `build_product_category_pairs` для получения всех пар «Имя продукта – Имя категории» (включая продукты без категорий) и набор тестов на `pytest`.

## Установка
```bash
python -m venv .venv
pip install -r requirements.txt
```

## Запуск тестов
```bash
pytest -q
```

## Пример использования
```python
from product_category import build_product_category_pairs

result = build_product_category_pairs(
    products_df,
    categories_df,
    product_categories_df,
    output_product_id_col="product_id",
    output_category_id_col="category_id",
    output_product_name_col="product",
    output_category_name_col="category",
    order_by=[("product", "asc"), ("category", "asc")],
)
```

## Параметры (основные)
- products_df, categories_df, product_categories_df — входные DataFrame.
- product_id_col, product_name_col — имена колонок id и имени продукта (по умолчанию: `id`, `name`).
- category_id_col, category_name_col — имена колонок id и имени категории (по умолчанию: `id`, `name`).
- pc_product_id_col, pc_category_id_col — имена колонок в связующей таблице (по умолчанию: `product_id`, `category_id`).
- output_product_id_col, output_category_id_col — имена id-колонок в результате (или None, чтобы не включать).
- output_product_name_col, output_category_name_col — имена колонок названий в результате.
- order_by — список пар (колонка, "asc"|"desc") для детерминированной сортировки результата.

