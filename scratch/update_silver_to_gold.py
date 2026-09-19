import json

with open('/home/miguelsb/workspace/visagio/Silver_to_Gold.ipynb', 'r', encoding='utf-8') as f:
    nb = json.load(f)

# Cell 1: imports and utils
nb['cells'][1]['source'] = '''import os
import sys
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    add_months,
    array_join,
    avg,
    coalesce,
    col,
    collect_set,
    concat,
    concat_ws,
    count,
    current_date,
    format_number,
    lit,
    rank,
    row_number,
    slice,
    trim,
    when,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DATA_DIR = Path.cwd() / "data"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

GOLD_DIR.mkdir(exist_ok=True)

DECIMAL_FINANCIAL_PRECISION = "decimal(18,2)"
SURROGATE_KEY_DATA_TYPE = "bigint"
STRING_NATURAL_KEY_TYPE = "string"
INTEGER_COUNT_TYPE = "int"
DOUBLE_METRIC_TYPE = "double"
DECIMAL_AVERAGE_SCALE = 2
MAX_PRIMARY_ACTORS_COUNT = 5

spark = (
    SparkSession
    .builder
    .appName("Silver_to_Gold")
    .getOrCreate()
)

def enforce_dataframe_schema(
    dataframe: DataFrame,
    expected_schema: StructType,
    strict_columns: bool = True
) -> DataFrame:
    """
    Valida a presença de todos os campos definidos no StructType, aplica conversão defensiva
    e reordena as colunas. Em modo estrito, impede colunas não declaradas no contrato.
    """
    actual_column_names = set(dataframe.columns)
    expected_column_names = [field.name for field in expected_schema.fields]
    missing_columns = set(expected_column_names) - actual_column_names

    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes no DataFrame: {missing_columns}")

    if strict_columns:
        unexpected_columns = actual_column_names - set(expected_column_names)
        if unexpected_columns:
            raise ValueError(f"Colunas imprevistas encontradas no DataFrame: {unexpected_columns}")

    ordered_column_expressions = [
        col(field.name).cast(field.dataType).alias(field.name)
        for field in expected_schema.fields
    ]
    return dataframe.select(ordered_column_expressions)

def write_dataframe(
    dataframe: DataFrame,
    target_path: Path,
    storage_format: str = "parquet",
    save_mode: str = "overwrite"
) -> None:
    """
    Persiste o DataFrame no formato especificado preservando o schema existente.
    """
    (
        dataframe.write
        .format(storage_format)
        .mode(save_mode)
        .save(str(target_path))
    )
'''.splitlines(keepends=True)

# Cell 3: dim_movies
nb['cells'][3]['source'] = '''@dataclass(frozen=True)
class SilverInfoFilmesInputColumns:
    MOVIE_ID: str = "id_filme"
    TITLE: str = "titulo"
    RELEASE_DATE: str = "data_lancamento"
    RELEASE_YEAR: str = "ano_lancamento"
    RUNTIME_MINUTES: str = "duracao_minutos"
    ORIGINAL_LANGUAGE: str = "idioma_original"
    STATUS: str = "status_filme"
    OVERVIEW: str = "sinopse"
    INGESTION_DATETIME: str = "ingestion_datetime"

@dataclass(frozen=True)
class DimMoviesColumns:
    SK_MOVIE_ID: str = "sk_movie_id"
    MOVIE_ID: str = "id_filme"
    TITLE: str = "titulo"
    RELEASE_DATE: str = "data_lancamento"
    RELEASE_YEAR: str = "ano_lancamento"
    RUNTIME_MINUTES: str = "duracao_minutos"
    ORIGINAL_LANGUAGE: str = "idioma_original"
    STATUS: str = "status_filme"
    OVERVIEW: str = "sinopse"
    INGESTION_DATETIME: str = "ingestion_datetime"

DimMoviesGoldSchema = StructType([
    StructField(DimMoviesColumns.SK_MOVIE_ID, LongType(), nullable=False),
    StructField(DimMoviesColumns.MOVIE_ID, StringType(), nullable=True),
    StructField(DimMoviesColumns.TITLE, StringType(), nullable=True),
    StructField(DimMoviesColumns.RELEASE_DATE, DateType(), nullable=True),
    StructField(DimMoviesColumns.RELEASE_YEAR, IntegerType(), nullable=True),
    StructField(DimMoviesColumns.RUNTIME_MINUTES, IntegerType(), nullable=True),
    StructField(DimMoviesColumns.ORIGINAL_LANGUAGE, StringType(), nullable=True),
    StructField(DimMoviesColumns.STATUS, StringType(), nullable=True),
    StructField(DimMoviesColumns.OVERVIEW, StringType(), nullable=True),
    StructField(DimMoviesColumns.INGESTION_DATETIME, TimestampType(), nullable=True),
])

dataframe_movies_info_silver = spark.read.parquet(str(SILVER_DIR / "silver.tb_info_filmes"))

def transform_dim_movies(raw_movies_info_dataframe: DataFrame) -> DataFrame:
    col_input_id = col(SilverInfoFilmesInputColumns.MOVIE_ID)
    col_input_title = col(SilverInfoFilmesInputColumns.TITLE)
    col_input_release_date = col(SilverInfoFilmesInputColumns.RELEASE_DATE)
    col_input_release_year = col(SilverInfoFilmesInputColumns.RELEASE_YEAR)
    col_input_runtime = col(SilverInfoFilmesInputColumns.RUNTIME_MINUTES)
    col_input_language = col(SilverInfoFilmesInputColumns.ORIGINAL_LANGUAGE)
    col_input_status = col(SilverInfoFilmesInputColumns.STATUS)
    col_input_overview = col(SilverInfoFilmesInputColumns.OVERVIEW)
    col_input_ingestion = col(SilverInfoFilmesInputColumns.INGESTION_DATETIME)

    window_order_by_natural_id = Window.orderBy(col_input_id)
    
    expr_sk_movie_id = row_number().over(window_order_by_natural_id).cast(SURROGATE_KEY_DATA_TYPE).alias(DimMoviesColumns.SK_MOVIE_ID)
    expr_movie_id = col_input_id.cast(STRING_NATURAL_KEY_TYPE).alias(DimMoviesColumns.MOVIE_ID)
    expr_title = col_input_title.alias(DimMoviesColumns.TITLE)
    expr_release_date = col_input_release_date.alias(DimMoviesColumns.RELEASE_DATE)
    expr_release_year = col_input_release_year.alias(DimMoviesColumns.RELEASE_YEAR)
    expr_runtime = col_input_runtime.alias(DimMoviesColumns.RUNTIME_MINUTES)
    expr_language = col_input_language.alias(DimMoviesColumns.ORIGINAL_LANGUAGE)
    expr_status = col_input_status.alias(DimMoviesColumns.STATUS)
    expr_overview = col_input_overview.alias(DimMoviesColumns.OVERVIEW)
    expr_ingestion = col_input_ingestion.alias(DimMoviesColumns.INGESTION_DATETIME)

    transformed_dataframe = (
        raw_movies_info_dataframe
        .select(
            expr_sk_movie_id,
            expr_movie_id,
            expr_title,
            expr_release_date,
            expr_release_year,
            expr_runtime,
            expr_language,
            expr_status,
            expr_overview,
            expr_ingestion
        )
    )
    return enforce_dataframe_schema(transformed_dataframe, DimMoviesGoldSchema)

dataframe_dim_movies = transform_dim_movies(dataframe_movies_info_silver)
write_dataframe(
    dataframe=dataframe_dim_movies,
    target_path=GOLD_DIR / "gold.dim_movies"
)
dataframe_dim_movies.printSchema()
dataframe_dim_movies.show(5, truncate=False)
'''.splitlines(keepends=True)

# Cell 5: dim_genres
nb['cells'][5]['source'] = '''@dataclass(frozen=True)
class SilverGenerosInputColumns:
    MOVIE_ID: str = "id_filme"
    GENRE_NAME: str = "nome_genero"
    INGESTION_DATETIME: str = "ingestion_datetime"

@dataclass(frozen=True)
class DimGenresColumns:
    SK_GENRE_ID: str = "sk_genre_id"
    GENRE_NAME: str = "nome_genero"
    INGESTION_DATETIME: str = "ingestion_datetime"

DimGenresGoldSchema = StructType([
    StructField(DimGenresColumns.SK_GENRE_ID, LongType(), nullable=False),
    StructField(DimGenresColumns.GENRE_NAME, StringType(), nullable=True),
    StructField(DimGenresColumns.INGESTION_DATETIME, TimestampType(), nullable=True),
])

dataframe_genres_silver = spark.read.parquet(str(SILVER_DIR / "silver.tb_generos"))

def transform_dim_genres(raw_genres_dataframe: DataFrame) -> DataFrame:
    col_input_genre = col(SilverGenerosInputColumns.GENRE_NAME)
    col_input_ingestion = col(SilverGenerosInputColumns.INGESTION_DATETIME)

    window_order_by_genre = Window.orderBy(col_input_genre)
    
    unique_genres_dataframe = (
        raw_genres_dataframe
        .groupBy(col_input_genre)
        .agg(spark_max(col_input_ingestion).alias(DimGenresColumns.INGESTION_DATETIME))
    )

    expr_sk_genre_id = row_number().over(window_order_by_genre).cast(SURROGATE_KEY_DATA_TYPE).alias(DimGenresColumns.SK_GENRE_ID)
    expr_genre_name = col_input_genre.alias(DimGenresColumns.GENRE_NAME)
    expr_ingestion = col(DimGenresColumns.INGESTION_DATETIME)

    transformed_dataframe = (
        unique_genres_dataframe
        .select(
            expr_sk_genre_id,
            expr_genre_name,
            expr_ingestion
        )
    )
    return enforce_dataframe_schema(transformed_dataframe, DimGenresGoldSchema)

dataframe_dim_genres = transform_dim_genres(dataframe_genres_silver)
write_dataframe(
    dataframe=dataframe_dim_genres,
    target_path=GOLD_DIR / "gold.dim_genres"
)
dataframe_dim_genres.printSchema()
dataframe_dim_genres.show(19, truncate=False)
'''.splitlines(keepends=True)

# Cell 7: dim_people
nb['cells'][7]['source'] = '''@dataclass(frozen=True)
class SilverPessoasEmpresasInputColumns:
    MOVIE_ID: str = "id_filme"
    ENTITY_NAME: str = "nome_entidade"
    ENTITY_TYPE: str = "tipo_entidade"
    INGESTION_DATETIME: str = "ingestion_datetime"

@dataclass(frozen=True)
class DimPeopleColumns:
    SK_PERSON_ID: str = "sk_person_id"
    PERSON_NAME: str = "nome_pessoa"
    PERSON_ROLE: str = "tipo_pessoa"
    INGESTION_DATETIME: str = "ingestion_datetime"

DimPeopleGoldSchema = StructType([
    StructField(DimPeopleColumns.SK_PERSON_ID, LongType(), nullable=False),
    StructField(DimPeopleColumns.PERSON_NAME, StringType(), nullable=True),
    StructField(DimPeopleColumns.PERSON_ROLE, StringType(), nullable=True),
    StructField(DimPeopleColumns.INGESTION_DATETIME, TimestampType(), nullable=True),
])

dataframe_entities_silver = spark.read.parquet(str(SILVER_DIR / "silver.tb_pessoas_empresas"))

def transform_dim_people(raw_entities_dataframe: DataFrame) -> DataFrame:
    target_physical_person_roles = ["Ator", "Diretor", "Roteirista"]
    
    col_input_name = col(SilverPessoasEmpresasInputColumns.ENTITY_NAME)
    col_input_type = col(SilverPessoasEmpresasInputColumns.ENTITY_TYPE)
    col_input_ingestion = col(SilverPessoasEmpresasInputColumns.INGESTION_DATETIME)

    window_order_by_person = Window.orderBy(col_input_name, col_input_type)
    
    filtered_people_dataframe = (
        raw_entities_dataframe
        .filter(col_input_type.isin(target_physical_person_roles))
        .groupBy(col_input_name, col_input_type)
        .agg(spark_max(col_input_ingestion).alias(DimPeopleColumns.INGESTION_DATETIME))
    )

    expr_sk_person_id = row_number().over(window_order_by_person).cast(SURROGATE_KEY_DATA_TYPE).alias(DimPeopleColumns.SK_PERSON_ID)
    expr_person_name = col_input_name.alias(DimPeopleColumns.PERSON_NAME)
    expr_person_role = col_input_type.alias(DimPeopleColumns.PERSON_ROLE)
    expr_ingestion = col(DimPeopleColumns.INGESTION_DATETIME)

    transformed_dataframe = (
        filtered_people_dataframe
        .select(
            expr_sk_person_id,
            expr_person_name,
            expr_person_role,
            expr_ingestion
        )
    )
    return enforce_dataframe_schema(transformed_dataframe, DimPeopleGoldSchema)

dataframe_dim_people = transform_dim_people(dataframe_entities_silver)
write_dataframe(
    dataframe=dataframe_dim_people,
    target_path=GOLD_DIR / "gold.dim_people"
)
dataframe_dim_people.printSchema()
dataframe_dim_people.show(5, truncate=False)
'''.splitlines(keepends=True)

# Cell 9: dim_companies
nb['cells'][9]['source'] = '''@dataclass(frozen=True)
class DimCompaniesColumns:
    SK_COMPANY_ID: str = "sk_company_id"
    COMPANY_NAME: str = "nome_produtora"
    INGESTION_DATETIME: str = "ingestion_datetime"

DimCompaniesGoldSchema = StructType([
    StructField(DimCompaniesColumns.SK_COMPANY_ID, LongType(), nullable=False),
    StructField(DimCompaniesColumns.COMPANY_NAME, StringType(), nullable=True),
    StructField(DimCompaniesColumns.INGESTION_DATETIME, TimestampType(), nullable=True),
])

def transform_dim_companies(raw_entities_dataframe: DataFrame) -> DataFrame:
    target_company_entity_type = "Produtora"
    
    col_input_name = col(SilverPessoasEmpresasInputColumns.ENTITY_NAME)
    col_input_type = col(SilverPessoasEmpresasInputColumns.ENTITY_TYPE)
    col_input_ingestion = col(SilverPessoasEmpresasInputColumns.INGESTION_DATETIME)

    window_order_by_company = Window.orderBy(col_input_name)
    
    filtered_companies_dataframe = (
        raw_entities_dataframe
        .filter(col_input_type == target_company_entity_type)
        .groupBy(col_input_name)
        .agg(spark_max(col_input_ingestion).alias(DimCompaniesColumns.INGESTION_DATETIME))
    )

    expr_sk_company_id = row_number().over(window_order_by_company).cast(SURROGATE_KEY_DATA_TYPE).alias(DimCompaniesColumns.SK_COMPANY_ID)
    expr_company_name = col_input_name.alias(DimCompaniesColumns.COMPANY_NAME)
    expr_ingestion = col(DimCompaniesColumns.INGESTION_DATETIME)

    transformed_dataframe = (
        filtered_companies_dataframe
        .select(
            expr_sk_company_id,
            expr_company_name,
            expr_ingestion
        )
    )
    return enforce_dataframe_schema(transformed_dataframe, DimCompaniesGoldSchema)

dataframe_dim_companies = transform_dim_companies(dataframe_entities_silver)
write_dataframe(
    dataframe=dataframe_dim_companies,
    target_path=GOLD_DIR / "gold.dim_companies"
)
dataframe_dim_companies.printSchema()
dataframe_dim_companies.show(5, truncate=False)
'''.splitlines(keepends=True)

# Cell 11: dim_reviews
nb['cells'][11]['source'] = '''@dataclass(frozen=True)
class SilverAvaliacoesInputColumns:
    MOVIE_ID: str = "id_filme"
    USER_NAME: str = "nome_usuario"
    USER_RATING: str = "nota_usuario"
    USER_COMMENT: str = "comentario_usuario"
    INGESTION_DATETIME: str = "ingestion_datetime"

@dataclass(frozen=True)
class DimReviewsColumns:
    SK_REVIEW_ID: str = "sk_review_id"
    SK_MOVIE_ID: str = "sk_movie_id"
    USER_REVIEWS_COUNT: str = "qtd_avaliacoes_usuarios"
    USER_AVERAGE_RATING: str = "nota_media_usuarios"
    INGESTION_DATETIME: str = "ingestion_datetime"

DimReviewsGoldSchema = StructType([
    StructField(DimReviewsColumns.SK_REVIEW_ID, LongType(), nullable=False),
    StructField(DimReviewsColumns.SK_MOVIE_ID, LongType(), nullable=True),
    StructField(DimReviewsColumns.USER_REVIEWS_COUNT, IntegerType(), nullable=True),
    StructField(DimReviewsColumns.USER_AVERAGE_RATING, DoubleType(), nullable=True),
    StructField(DimReviewsColumns.INGESTION_DATETIME, TimestampType(), nullable=True),
])

dataframe_reviews_silver = spark.read.parquet(str(SILVER_DIR / "silver.tb_avaliacoes_usuarios"))

def transform_dim_reviews(raw_reviews_dataframe: DataFrame, canonical_movies_dataframe: DataFrame) -> DataFrame:
    col_input_movie_id = raw_reviews_dataframe[SilverAvaliacoesInputColumns.MOVIE_ID].cast(STRING_NATURAL_KEY_TYPE)
    col_target_movie_id = canonical_movies_dataframe[DimMoviesColumns.MOVIE_ID]
    col_user_rating = col(SilverAvaliacoesInputColumns.USER_RATING)
    col_input_ingestion = col(SilverAvaliacoesInputColumns.INGESTION_DATETIME)

    reviews_with_foreign_key = (
        raw_reviews_dataframe
        .join(
            canonical_movies_dataframe.select(DimMoviesColumns.SK_MOVIE_ID, DimMoviesColumns.MOVIE_ID),
            col_input_movie_id == col_target_movie_id,
            "inner"
        )
    )
    
    col_sk_movie_id = col(DimReviewsColumns.SK_MOVIE_ID)
    window_order_by_movie_foreign_key = Window.orderBy(col_sk_movie_id)

    aggregated_reviews = (
        reviews_with_foreign_key
        .groupBy(col_sk_movie_id)
        .agg(
            count(col_user_rating).cast(INTEGER_COUNT_TYPE).alias(DimReviewsColumns.USER_REVIEWS_COUNT),
            spark_round(avg(col_user_rating), DECIMAL_AVERAGE_SCALE).cast(DOUBLE_METRIC_TYPE).alias(DimReviewsColumns.USER_AVERAGE_RATING),
            spark_max(col_input_ingestion).alias(DimReviewsColumns.INGESTION_DATETIME)
        )
    )

    expr_sk_review_id = row_number().over(window_order_by_movie_foreign_key).cast(SURROGATE_KEY_DATA_TYPE).alias(DimReviewsColumns.SK_REVIEW_ID)
    expr_sk_movie_id = col_sk_movie_id
    expr_user_reviews_count = col(DimReviewsColumns.USER_REVIEWS_COUNT)
    expr_user_avg_rating = col(DimReviewsColumns.USER_AVERAGE_RATING)
    expr_ingestion = col(DimReviewsColumns.INGESTION_DATETIME)

    transformed_dataframe = (
        aggregated_reviews
        .select(
            expr_sk_review_id,
            expr_sk_movie_id,
            expr_user_reviews_count,
            expr_user_avg_rating,
            expr_ingestion
        )
    )
    return enforce_dataframe_schema(transformed_dataframe, DimReviewsGoldSchema)

dataframe_dim_reviews = transform_dim_reviews(dataframe_reviews_silver, dataframe_dim_movies)
write_dataframe(
    dataframe=dataframe_dim_reviews,
    target_path=GOLD_DIR / "gold.dim_reviews"
)
dataframe_dim_reviews.printSchema()
dataframe_dim_reviews.show(5, truncate=False)
'''.splitlines(keepends=True)

# Cell 13: bridge tables
nb['cells'][13]['source'] = '''@dataclass(frozen=True)
class BridgeMovieGenreColumns:
    SK_MOVIE_ID: str = "sk_movie_id"
    SK_GENRE_ID: str = "sk_genre_id"
    INGESTION_DATETIME: str = "ingestion_datetime"

@dataclass(frozen=True)
class BridgeMoviePersonColumns:
    SK_MOVIE_ID: str = "sk_movie_id"
    SK_PERSON_ID: str = "sk_person_id"
    INGESTION_DATETIME: str = "ingestion_datetime"

@dataclass(frozen=True)
class BridgeMovieCompanyColumns:
    SK_MOVIE_ID: str = "sk_movie_id"
    SK_COMPANY_ID: str = "sk_company_id"
    INGESTION_DATETIME: str = "ingestion_datetime"

BridgeMovieGenreGoldSchema = StructType([
    StructField(BridgeMovieGenreColumns.SK_MOVIE_ID, LongType(), nullable=False),
    StructField(BridgeMovieGenreColumns.SK_GENRE_ID, LongType(), nullable=False),
    StructField(BridgeMovieGenreColumns.INGESTION_DATETIME, TimestampType(), nullable=True),
])

BridgeMoviePersonGoldSchema = StructType([
    StructField(BridgeMoviePersonColumns.SK_MOVIE_ID, LongType(), nullable=False),
    StructField(BridgeMoviePersonColumns.SK_PERSON_ID, LongType(), nullable=False),
    StructField(BridgeMoviePersonColumns.INGESTION_DATETIME, TimestampType(), nullable=True),
])

BridgeMovieCompanyGoldSchema = StructType([
    StructField(BridgeMovieCompanyColumns.SK_MOVIE_ID, LongType(), nullable=False),
    StructField(BridgeMovieCompanyColumns.SK_COMPANY_ID, LongType(), nullable=False),
    StructField(BridgeMovieCompanyColumns.INGESTION_DATETIME, TimestampType(), nullable=True),
])

def build_bridge_movie_genre(
    raw_genres_dataframe: DataFrame,
    canonical_movies_dataframe: DataFrame,
    canonical_genres_dataframe: DataFrame
) -> DataFrame:
    col_movie_id_raw = raw_genres_dataframe[SilverGenerosInputColumns.MOVIE_ID].cast(STRING_NATURAL_KEY_TYPE)
    col_movie_id_canonical = canonical_movies_dataframe[DimMoviesColumns.MOVIE_ID]
    col_genre_name = SilverGenerosInputColumns.GENRE_NAME
    col_ingestion = raw_genres_dataframe[SilverGenerosInputColumns.INGESTION_DATETIME]

    transformed_dataframe = (
        raw_genres_dataframe
        .join(canonical_movies_dataframe.select(DimMoviesColumns.SK_MOVIE_ID, DimMoviesColumns.MOVIE_ID), col_movie_id_raw == col_movie_id_canonical, "inner")
        .join(canonical_genres_dataframe, col_genre_name, "inner")
        .select(
            col(DimMoviesColumns.SK_MOVIE_ID).alias(BridgeMovieGenreColumns.SK_MOVIE_ID),
            col(DimGenresColumns.SK_GENRE_ID).alias(BridgeMovieGenreColumns.SK_GENRE_ID),
            col_ingestion.alias(BridgeMovieGenreColumns.INGESTION_DATETIME)
        )
        .distinct()
    )
    return enforce_dataframe_schema(transformed_dataframe, BridgeMovieGenreGoldSchema)

def build_bridge_movie_person(
    raw_entities_dataframe: DataFrame,
    canonical_movies_dataframe: DataFrame,
    canonical_people_dataframe: DataFrame
) -> DataFrame:
    target_physical_person_roles = ["Ator", "Diretor", "Roteirista"]
    filtered_people_entities = raw_entities_dataframe.filter(col(SilverPessoasEmpresasInputColumns.ENTITY_TYPE).isin(target_physical_person_roles))
    
    col_movie_id_raw = filtered_people_entities[SilverPessoasEmpresasInputColumns.MOVIE_ID].cast(STRING_NATURAL_KEY_TYPE)
    col_movie_id_canonical = canonical_movies_dataframe[DimMoviesColumns.MOVIE_ID]
    col_ingestion = filtered_people_entities[SilverPessoasEmpresasInputColumns.INGESTION_DATETIME]

    col_person_join_condition = (
        (filtered_people_entities[SilverPessoasEmpresasInputColumns.ENTITY_NAME] == canonical_people_dataframe[DimPeopleColumns.PERSON_NAME]) &
        (filtered_people_entities[SilverPessoasEmpresasInputColumns.ENTITY_TYPE] == canonical_people_dataframe[DimPeopleColumns.PERSON_ROLE])
    )
    transformed_dataframe = (
        filtered_people_entities
        .join(canonical_movies_dataframe.select(DimMoviesColumns.SK_MOVIE_ID, DimMoviesColumns.MOVIE_ID), col_movie_id_raw == col_movie_id_canonical, "inner")
        .join(canonical_people_dataframe, col_person_join_condition, "inner")
        .select(
            col(DimMoviesColumns.SK_MOVIE_ID).alias(BridgeMoviePersonColumns.SK_MOVIE_ID),
            col(DimPeopleColumns.SK_PERSON_ID).alias(BridgeMoviePersonColumns.SK_PERSON_ID),
            col_ingestion.alias(BridgeMoviePersonColumns.INGESTION_DATETIME)
        )
        .distinct()
    )
    return enforce_dataframe_schema(transformed_dataframe, BridgeMoviePersonGoldSchema)

def build_bridge_movie_company(
    raw_entities_dataframe: DataFrame,
    canonical_movies_dataframe: DataFrame,
    canonical_companies_dataframe: DataFrame
) -> DataFrame:
    target_company_entity_type = "Produtora"
    filtered_company_entities = raw_entities_dataframe.filter(col(SilverPessoasEmpresasInputColumns.ENTITY_TYPE) == target_company_entity_type)
    
    col_movie_id_raw = filtered_company_entities[SilverPessoasEmpresasInputColumns.MOVIE_ID].cast(STRING_NATURAL_KEY_TYPE)
    col_movie_id_canonical = canonical_movies_dataframe[DimMoviesColumns.MOVIE_ID]
    col_company_join = filtered_company_entities[SilverPessoasEmpresasInputColumns.ENTITY_NAME] == canonical_companies_dataframe[DimCompaniesColumns.COMPANY_NAME]
    col_ingestion = filtered_company_entities[SilverPessoasEmpresasInputColumns.INGESTION_DATETIME]

    transformed_dataframe = (
        filtered_company_entities
        .join(canonical_movies_dataframe.select(DimMoviesColumns.SK_MOVIE_ID, DimMoviesColumns.MOVIE_ID), col_movie_id_raw == col_movie_id_canonical, "inner")
        .join(canonical_companies_dataframe, col_company_join, "inner")
        .select(
            col(DimMoviesColumns.SK_MOVIE_ID).alias(BridgeMovieCompanyColumns.SK_MOVIE_ID),
            col(DimCompaniesColumns.SK_COMPANY_ID).alias(BridgeMovieCompanyColumns.SK_COMPANY_ID),
            col_ingestion.alias(BridgeMovieCompanyColumns.INGESTION_DATETIME)
        )
        .distinct()
    )
    return enforce_dataframe_schema(transformed_dataframe, BridgeMovieCompanyGoldSchema)

dataframe_bridge_movie_genre = build_bridge_movie_genre(dataframe_genres_silver, dataframe_dim_movies, dataframe_dim_genres)
dataframe_bridge_movie_person = build_bridge_movie_person(dataframe_entities_silver, dataframe_dim_movies, dataframe_dim_people)
dataframe_bridge_movie_company = build_bridge_movie_company(dataframe_entities_silver, dataframe_dim_movies, dataframe_dim_companies)

write_dataframe(dataframe_bridge_movie_genre, GOLD_DIR / "gold.bridge_movie_genre")
write_dataframe(dataframe_bridge_movie_person, GOLD_DIR / "gold.bridge_movie_person")
write_dataframe(dataframe_bridge_movie_company, GOLD_DIR / "gold.bridge_movie_company")

dataframe_bridge_movie_genre.printSchema()
dataframe_bridge_movie_person.printSchema()
dataframe_bridge_movie_company.printSchema()
'''.splitlines(keepends=True)

# Cell 15: fact_movies_performance
nb['cells'][15]['source'] = '''@dataclass(frozen=True)
class SilverFinancialsInputColumns:
    MOVIE_ID: str = "id_filme"
    BUDGET_USD: str = "orcamento_usd"
    REVENUE_USD: str = "receita_usd"
    BUDGET_BRL: str = "orcamento_brl"
    REVENUE_BRL: str = "receita_brl"
    PROFIT_USD: str = "lucro_usd"
    PROFIT_BRL: str = "lucro_brl"
    PROFIT_MARGIN_PERCENT: str = "margem_lucro_percentual"
    INGESTION_DATETIME: str = "ingestion_datetime"

@dataclass(frozen=True)
class SilverMetricsInputColumns:
    MOVIE_ID: str = "id_filme"
    POPULARITY: str = "popularidade"
    VOTE_AVERAGE_TMDB: str = "nota_media_tmdb"
    VOTE_COUNT_TMDB: str = "qtd_votos_tmdb"
    VOTE_AVERAGE_IMDB: str = "nota_media_imdb"
    VOTE_COUNT_IMDB: str = "qtd_votos_imdb"
    INGESTION_DATETIME: str = "ingestion_datetime"

@dataclass(frozen=True)
class FactMoviesPerformanceColumns:
    SK_MOVIE_ID: str = "sk_movie_id"
    BUDGET_USD: str = "orcamento_usd"
    REVENUE_USD: str = "receita_usd"
    PROFIT_USD: str = "lucro_usd"
    BUDGET_BRL: str = "orcamento_brl"
    REVENUE_BRL: str = "receita_brl"
    PROFIT_BRL: str = "lucro_brl"
    POPULARITY: str = "popularidade"
    VOTE_AVERAGE_TMDB: str = "nota_media_tmdb"
    VOTE_COUNT_TMDB: str = "qtd_votos_tmdb"
    VOTE_AVERAGE_IMDB: str = "nota_media_imdb"
    VOTE_COUNT_IMDB: str = "qtd_votos_imdb"
    INGESTION_DATETIME: str = "ingestion_datetime"

FactMoviesPerformanceGoldSchema = StructType([
    StructField(FactMoviesPerformanceColumns.SK_MOVIE_ID, LongType(), nullable=False),
    StructField(FactMoviesPerformanceColumns.BUDGET_USD, DecimalType(18, 2), nullable=True),
    StructField(FactMoviesPerformanceColumns.REVENUE_USD, DecimalType(18, 2), nullable=True),
    StructField(FactMoviesPerformanceColumns.PROFIT_USD, DecimalType(18, 2), nullable=True),
    StructField(FactMoviesPerformanceColumns.BUDGET_BRL, DecimalType(18, 2), nullable=True),
    StructField(FactMoviesPerformanceColumns.REVENUE_BRL, DecimalType(18, 2), nullable=True),
    StructField(FactMoviesPerformanceColumns.PROFIT_BRL, DecimalType(18, 2), nullable=True),
    StructField(FactMoviesPerformanceColumns.POPULARITY, DoubleType(), nullable=True),
    StructField(FactMoviesPerformanceColumns.VOTE_AVERAGE_TMDB, DoubleType(), nullable=True),
    StructField(FactMoviesPerformanceColumns.VOTE_COUNT_TMDB, IntegerType(), nullable=True),
    StructField(FactMoviesPerformanceColumns.VOTE_AVERAGE_IMDB, DoubleType(), nullable=True),
    StructField(FactMoviesPerformanceColumns.VOTE_COUNT_IMDB, IntegerType(), nullable=True),
    StructField(FactMoviesPerformanceColumns.INGESTION_DATETIME, TimestampType(), nullable=True),
])

dataframe_financials_silver = spark.read.parquet(str(SILVER_DIR / "silver.tb_financeiro_filmes"))
dataframe_metrics_silver = spark.read.parquet(str(SILVER_DIR / "silver.tb_metricas_engajamento"))

def transform_fact_movies_performance(
    canonical_movies_dataframe: DataFrame,
    raw_financials_dataframe: DataFrame,
    raw_metrics_dataframe: DataFrame
) -> DataFrame:
    financials_renamed = raw_financials_dataframe.withColumnRenamed(SilverFinancialsInputColumns.MOVIE_ID, "id_filme_financeiro")
    metrics_renamed = raw_metrics_dataframe.withColumnRenamed(SilverMetricsInputColumns.MOVIE_ID, "id_filme_metricas")
    
    col_movie_id_canonical = canonical_movies_dataframe[DimMoviesColumns.MOVIE_ID]
    col_movie_id_financial = financials_renamed["id_filme_financeiro"].cast(STRING_NATURAL_KEY_TYPE)
    col_movie_id_metric = metrics_renamed["id_filme_metricas"].cast(STRING_NATURAL_KEY_TYPE)

    col_financial_join_condition = col_movie_id_canonical == col_movie_id_financial
    col_metrics_join_condition = col_movie_id_canonical == col_movie_id_metric
    
    col_sk_movie_id = col(DimMoviesColumns.SK_MOVIE_ID)
    col_budget_usd = col(SilverFinancialsInputColumns.BUDGET_USD)
    col_revenue_usd = col(SilverFinancialsInputColumns.REVENUE_USD)
    col_profit_usd = col(SilverFinancialsInputColumns.PROFIT_USD)
    col_budget_brl = col(SilverFinancialsInputColumns.BUDGET_BRL)
    col_revenue_brl = col(SilverFinancialsInputColumns.REVENUE_BRL)
    col_profit_brl = col(SilverFinancialsInputColumns.PROFIT_BRL)
    col_popularity = col(SilverMetricsInputColumns.POPULARITY)
    col_vote_avg_tmdb = col(SilverMetricsInputColumns.VOTE_AVERAGE_TMDB)
    col_vote_cnt_tmdb = col(SilverMetricsInputColumns.VOTE_COUNT_TMDB)
    col_vote_avg_imdb = col(SilverMetricsInputColumns.VOTE_AVERAGE_IMDB)
    col_vote_cnt_imdb = col(SilverMetricsInputColumns.VOTE_COUNT_IMDB)
    col_ingestion = coalesce(
        financials_renamed[SilverFinancialsInputColumns.INGESTION_DATETIME],
        metrics_renamed[SilverMetricsInputColumns.INGESTION_DATETIME],
        canonical_movies_dataframe[DimMoviesColumns.INGESTION_DATETIME]
    )

    expr_sk_movie_id = col_sk_movie_id.alias(FactMoviesPerformanceColumns.SK_MOVIE_ID)
    expr_budget_usd = col_budget_usd.cast(DECIMAL_FINANCIAL_PRECISION).alias(FactMoviesPerformanceColumns.BUDGET_USD)
    expr_revenue_usd = col_revenue_usd.cast(DECIMAL_FINANCIAL_PRECISION).alias(FactMoviesPerformanceColumns.REVENUE_USD)
    expr_profit_usd = col_profit_usd.cast(DECIMAL_FINANCIAL_PRECISION).alias(FactMoviesPerformanceColumns.PROFIT_USD)
    expr_budget_brl = col_budget_brl.cast(DECIMAL_FINANCIAL_PRECISION).alias(FactMoviesPerformanceColumns.BUDGET_BRL)
    expr_revenue_brl = col_revenue_brl.cast(DECIMAL_FINANCIAL_PRECISION).alias(FactMoviesPerformanceColumns.REVENUE_BRL)
    expr_profit_brl = col_profit_brl.cast(DECIMAL_FINANCIAL_PRECISION).alias(FactMoviesPerformanceColumns.PROFIT_BRL)
    expr_popularity = col_popularity.cast(DOUBLE_METRIC_TYPE).alias(FactMoviesPerformanceColumns.POPULARITY)
    expr_vote_avg_tmdb = col_vote_avg_tmdb.cast(DOUBLE_METRIC_TYPE).alias(FactMoviesPerformanceColumns.VOTE_AVERAGE_TMDB)
    expr_vote_cnt_tmdb = col_vote_cnt_tmdb.cast(INTEGER_COUNT_TYPE).alias(FactMoviesPerformanceColumns.VOTE_COUNT_TMDB)
    expr_vote_avg_imdb = col_vote_avg_imdb.cast(DOUBLE_METRIC_TYPE).alias(FactMoviesPerformanceColumns.VOTE_AVERAGE_IMDB)
    expr_vote_cnt_imdb = col_vote_cnt_imdb.cast(INTEGER_COUNT_TYPE).alias(FactMoviesPerformanceColumns.VOTE_COUNT_IMDB)
    expr_ingestion = col_ingestion.alias(FactMoviesPerformanceColumns.INGESTION_DATETIME)

    transformed_dataframe = (
        canonical_movies_dataframe.select(DimMoviesColumns.SK_MOVIE_ID, DimMoviesColumns.MOVIE_ID, DimMoviesColumns.INGESTION_DATETIME)
        .join(financials_renamed, col_financial_join_condition, "left")
        .join(metrics_renamed, col_metrics_join_condition, "left")
        .select(
            expr_sk_movie_id,
            expr_budget_usd,
            expr_revenue_usd,
            expr_profit_usd,
            expr_budget_brl,
            expr_revenue_brl,
            expr_profit_brl,
            expr_popularity,
            expr_vote_avg_tmdb,
            expr_vote_cnt_tmdb,
            expr_vote_avg_imdb,
            expr_vote_cnt_imdb,
            expr_ingestion
        )
    )
    return enforce_dataframe_schema(transformed_dataframe, FactMoviesPerformanceGoldSchema)

dataframe_fact_movies_performance = transform_fact_movies_performance(
    canonical_movies_dataframe=dataframe_dim_movies,
    raw_financials_dataframe=dataframe_financials_silver,
    raw_metrics_dataframe=dataframe_metrics_silver
)
write_dataframe(
    dataframe=dataframe_fact_movies_performance,
    target_path=GOLD_DIR / "gold.fact_movies_performance"
)
dataframe_fact_movies_performance.printSchema()
dataframe_fact_movies_performance.show(5, truncate=False)
'''.splitlines(keepends=True)

# Cell 17: genai context
nb['cells'][17]['source'] = '''@dataclass(frozen=True)
class GenAiMoviesContextColumns:
    MOVIE_ID: str = "movie_id"
    TITLE: str = "title"
    LLM_CONTEXT_DOCUMENT: str = "llm_context_document"
    INGESTION_DATETIME: str = "ingestion_datetime"

GoldGenaiMoviesContextSchema = StructType([
    StructField(GenAiMoviesContextColumns.MOVIE_ID, StringType(), nullable=True),
    StructField(GenAiMoviesContextColumns.TITLE, StringType(), nullable=True),
    StructField(GenAiMoviesContextColumns.LLM_CONTEXT_DOCUMENT, StringType(), nullable=True),
    StructField(GenAiMoviesContextColumns.INGESTION_DATETIME, TimestampType(), nullable=True),
])

def transform_genai_movies_context(
    canonical_movies_dataframe: DataFrame,
    performance_fact_dataframe: DataFrame,
    bridge_person_dataframe: DataFrame,
    canonical_people_dataframe: DataFrame
) -> DataFrame:
    people_with_role = bridge_person_dataframe.join(canonical_people_dataframe, DimPeopleColumns.SK_PERSON_ID, "inner")
    
    col_sk_movie_id = DimMoviesColumns.SK_MOVIE_ID
    col_person_role = DimPeopleColumns.PERSON_ROLE
    col_person_name = DimPeopleColumns.PERSON_NAME

    # Agrega os atores principais limitados aos participantes de destaque
    actors_aggregated = (
        people_with_role
        .filter(col(col_person_role) == "Ator")
        .groupBy(col_sk_movie_id)
        .agg(array_join(slice(collect_set(col_person_name), 1, MAX_PRIMARY_ACTORS_COUNT), ", ").alias("atores_principais"))
    )
    
    directors_aggregated = (
        people_with_role
        .filter(col(col_person_role) == "Diretor")
        .groupBy(col_sk_movie_id)
        .agg(concat_ws(", ", collect_set(col_person_name)).alias("diretores"))
    )
    
    col_raw_title = col(DimMoviesColumns.TITLE)
    col_raw_year = col(DimMoviesColumns.RELEASE_YEAR)
    col_raw_revenue = col(FactMoviesPerformanceColumns.REVENUE_USD)
    col_raw_budget = col(FactMoviesPerformanceColumns.BUDGET_USD)
    col_raw_actors = col("atores_principais")
    col_raw_directors = col("diretores")
    col_raw_overview = col(DimMoviesColumns.OVERVIEW)
    col_raw_ingestion = canonical_movies_dataframe[DimMoviesColumns.INGESTION_DATETIME]
    
    # Aplica camadas de fallback para proteção estrita contra nulos
    formatted_title_expression = coalesce(trim(col_raw_title), lit("Título não informado"))
    formatted_year_expression = when(col_raw_year.isNotNull(), col_raw_year.cast(STRING_NATURAL_KEY_TYPE)).otherwise(lit("ano não informado"))
    formatted_revenue_expression = when(
        col_raw_revenue.isNotNull() & (col_raw_revenue > 0),
        concat(lit("US$ "), format_number(col_raw_revenue, DECIMAL_AVERAGE_SCALE))
    ).otherwise(lit("valor não informado"))
    formatted_budget_expression = when(
        col_raw_budget.isNotNull() & (col_raw_budget > 0),
        concat(lit("US$ "), format_number(col_raw_budget, DECIMAL_AVERAGE_SCALE))
    ).otherwise(lit("valor não informado"))
    formatted_actors_expression = when(col_raw_actors.isNotNull() & (trim(col_raw_actors) != ""), trim(col_raw_actors)).otherwise(lit("elenco não informado"))
    formatted_directors_expression = when(col_raw_directors.isNotNull() & (trim(col_raw_directors) != ""), trim(col_raw_directors)).otherwise(lit("diretor não informado"))
    formatted_overview_expression = coalesce(trim(col_raw_overview), lit("Sinopse não disponível."))
    
    expr_movie_id = col(DimMoviesColumns.MOVIE_ID).alias(GenAiMoviesContextColumns.MOVIE_ID)
    expr_title = col(DimMoviesColumns.TITLE).alias(GenAiMoviesContextColumns.TITLE)
    expr_llm_doc = col("llm_context_document").alias(GenAiMoviesContextColumns.LLM_CONTEXT_DOCUMENT)
    expr_ingestion = col_raw_ingestion.alias(GenAiMoviesContextColumns.INGESTION_DATETIME)

    transformed_dataframe = (
        canonical_movies_dataframe.select(DimMoviesColumns.SK_MOVIE_ID, DimMoviesColumns.MOVIE_ID, DimMoviesColumns.TITLE, DimMoviesColumns.RELEASE_YEAR, DimMoviesColumns.OVERVIEW, DimMoviesColumns.INGESTION_DATETIME)
        .join(performance_fact_dataframe.select(FactMoviesPerformanceColumns.SK_MOVIE_ID, FactMoviesPerformanceColumns.REVENUE_USD, FactMoviesPerformanceColumns.BUDGET_USD), DimMoviesColumns.SK_MOVIE_ID, "left")
        .join(actors_aggregated, DimMoviesColumns.SK_MOVIE_ID, "left")
        .join(directors_aggregated, DimMoviesColumns.SK_MOVIE_ID, "left")
        .withColumn("texto_titulo", formatted_title_expression)
        .withColumn("texto_ano", formatted_year_expression)
        .withColumn("texto_receita", formatted_revenue_expression)
        .withColumn("texto_orcamento", formatted_budget_expression)
        .withColumn("texto_atores", formatted_actors_expression)
        .withColumn("texto_diretores", formatted_directors_expression)
        .withColumn("texto_sinopse", formatted_overview_expression)
        .withColumn(
            "llm_context_document",
            concat(
                lit("O filme "), col("texto_titulo"),
                lit(", lançado no ano de "), col("texto_ano"),
                lit(", faturou "), col("texto_receita"),
                lit(" e teve um custo de "), col("texto_orcamento"),
                lit(". Estrelado por "), col("texto_atores"),
                lit(" e dirigido por "), col("texto_diretores"),
                lit(", o filme possui a seguinte sinopse: "), col("texto_sinopse")
            )
        )
        .select(
            expr_movie_id,
            expr_title,
            expr_llm_doc,
            expr_ingestion
        )
    )
    return enforce_dataframe_schema(transformed_dataframe, GoldGenaiMoviesContextSchema)

dataframe_gold_genai_movies_context = transform_genai_movies_context(
    canonical_movies_dataframe=dataframe_dim_movies,
    performance_fact_dataframe=dataframe_fact_movies_performance,
    bridge_person_dataframe=dataframe_bridge_movie_person,
    canonical_people_dataframe=dataframe_dim_people
)
write_dataframe(
    dataframe=dataframe_gold_genai_movies_context,
    target_path=GOLD_DIR / "gold.gold_genai_movies_context"
)
dataframe_gold_genai_movies_context.printSchema()
dataframe_gold_genai_movies_context.show(5, truncate=False)
'''.splitlines(keepends=True)

with open('/home/miguelsb/workspace/visagio/Silver_to_Gold.ipynb', 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

print('Updated Silver_to_Gold.ipynb successfully')
