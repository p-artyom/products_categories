from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = (
        SparkSession.builder.master('local')
        .appName('products_category')
        .getOrCreate()
    )

    products = spark.read.csv('data/products.csv', header=True)
    categories = spark.read.csv('data/categories.csv', header=True)
    product_category = spark.read.csv('data/product_category.csv', header=True)

    products.join(
        product_category,
        products.id == product_category.product_id,
        how='left',
    ).join(
        categories, product_category.category_id == categories.id, how='left'
    ).select(
        ['product_name', 'category_name']
    ).show(
        products.count()
    )
