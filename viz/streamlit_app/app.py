import os
import pandas as pd
import plotly.express as px
import streamlit as st
from google.cloud import bigquery

# ---- Config ----
DEFAULT_PROJECT = os.getenv("PROJECT_ID", "upheld-caldron-468622-n6")
DATASET = "dw_imdb_agg"

@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=DEFAULT_PROJECT)

@st.cache_data(ttl=600)
def load_year_bounds():
    q = f"""
    SELECT
      MIN(start_year) AS min_y,
      MAX(start_year) AS max_y
    FROM `{DEFAULT_PROJECT}.{DATASET}.genre_year`
    """
    return get_bq_client().query(q).to_dataframe().iloc[0].to_dict()

@st.cache_data(ttl=600)
def load_top_genres(limit=10, min_year=None, max_year=None):
    where = []
    if min_year is not None and max_year is not None:
        where.append(f"start_year BETWEEN {int(min_year)} AND {int(max_year)}")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    q = f"""
    SELECT genre, SUM(movie_count) AS total_movies
    FROM `{DEFAULT_PROJECT}.{DATASET}.genre_year`
    {where_sql}
    GROUP BY genre
    ORDER BY total_movies DESC
    LIMIT {int(limit)}
    """
    return get_bq_client().query(q).to_dataframe()

@st.cache_data(ttl=600)
def query_genre_trend(genres, min_year, max_year):
    genres_list = ",".join([f"'{g}'" for g in genres]) if genres else "''"
    q = f"""
    SELECT start_year, genre, movie_count, weighted_avg_rating
    FROM `{DEFAULT_PROJECT}.{DATASET}.genre_year`
    WHERE start_year BETWEEN {int(min_year)} AND {int(max_year)}
      AND genre IN ({genres_list})
    ORDER BY start_year, genre
    """
    return get_bq_client().query(q, location="US").to_dataframe()

@st.cache_data(ttl=600)
def query_director_leaderboard(limit=15):
    q = f"""
    SELECT primary_name AS director_name,
           film_count,
           total_votes,
           ROUND(weighted_avg_rating, 3) AS weighted_avg_rating
    FROM `{DEFAULT_PROJECT}.{DATASET}.director_stats`
    ORDER BY total_votes DESC, weighted_avg_rating DESC
    LIMIT {int(limit)}
    """
    return get_bq_client().query(q).to_dataframe()

@st.cache_data(ttl=600)
def query_top_movie_by_year():
    q = f"""
    SELECT start_year, title, averageRating, numVotes
    FROM `{DEFAULT_PROJECT}.{DATASET}.top_movie_by_year`
    ORDER BY start_year
    """
    return get_bq_client().query(q).to_dataframe()

# ---- UI ----
st.set_page_config(page_title="IMDb DW — Task 4", layout="wide")
st.title("Task 4 — IMDb Data Visualization (DW Aggregates)")

st.sidebar.header("Controls")
st.sidebar.write(f"Project: **{DEFAULT_PROJECT}**  •  Dataset: **{DATASET}**")

yb = load_year_bounds()
year_min = int(yb["min_y"] or 1870)
year_max = int(yb["max_y"] or 2035)
yr_from, yr_to = st.sidebar.slider("Year range", year_min, year_max, (2000, year_max), step=1)
topg = load_top_genres(limit=10, min_year=yr_from, max_year=yr_to)
default_genres = topg["genre"].tolist()
genres = st.sidebar.multiselect("Genres", options=default_genres, default=default_genres)

st.markdown("### 1) Genre volume over time")
gt = query_genre_trend(genres=genres, min_year=yr_from, max_year=yr_to)
if gt.empty:
    st.info("No data for the current selection.")
else:
    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(gt, x="start_year", y="movie_count", color="genre",
                      labels={"start_year":"Year","movie_count":"# Titles","genre":"Genre"},
                      title="Titles per year (selected genres)")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig2 = px.line(gt, x="start_year", y="weighted_avg_rating", color="genre",
                       labels={"start_year":"Year","weighted_avg_rating":"Weighted Rating","genre":"Genre"},
                       title="Weighted avg rating per year (selected genres)")
        st.plotly_chart(fig2, use_container_width=True)

st.markdown("### 2) Director leaderboard (engagement & quality)")
dl = query_director_leaderboard(limit=15)
fig3 = px.bar(dl, x="total_votes", y="director_name", orientation="h",
              hover_data=["film_count","weighted_avg_rating"],
              labels={"total_votes":"Total Votes","director_name":"Director"},
              title="Top Directors by Total Votes")
st.plotly_chart(fig3, use_container_width=True)

st.markdown("### 3) Top movie by year")
tby = query_top_movie_by_year()
st.dataframe(tby.tail(15), use_container_width=True)

# Downloads
st.download_button("Download director leaderboard (CSV)", dl.to_csv(index=False).encode("utf-8"),
                   file_name="director_leaderboard.csv", mime="text/csv")
st.download_button("Download genre trend (CSV)", gt.to_csv(index=False).encode("utf-8"),
                   file_name="genre_trend.csv", mime="text/csv")
