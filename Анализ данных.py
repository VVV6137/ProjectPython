import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams['figure.figsize'] = (15, 10)
plt.rcParams['font.size'] = 12

df = pd.read_csv('imdb.csv')

column_mapping = {}
if 'Data' in df.columns:
    column_mapping['Data'] = 'Date'
if 'Nudity, violence..' in df.columns:
    column_mapping['Nudity, violence..'] = 'Content_Rating'

df = df.rename(columns=column_mapping)

initial_count = len(df)
df = df[df['Rate'] != 'No rate'].copy()

df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce')

df = df.dropna(subset=['Rate'])

if 'Votes' in df.columns:
    df['Votes'] = pd.to_numeric(df['Votes'], errors='coerce')


if 'Episodes' in df.columns:
    df['Episodes'] = pd.to_numeric(df['Episodes'], errors='coerce')
    df['Episodes'] = df['Episodes'].fillna(1)

if 'Genre' in df.columns:
    df['Genre'] = df['Genre'].str.replace('Actions', 'Action')
    df['Genre_Clean'] = df['Genre'].str.split(', ')

if 'Type' in df.columns and 'Episodes' in df.columns:
    df['Series_Length'] = 'Not Series'
    series_mask = df['Type'] == 'Series'

    df.loc[series_mask & (df['Episodes'] <= 20), 'Series_Length'] = 'Короткие (1-20 эп.)'
    df.loc[series_mask & (df['Episodes'] > 20) & (df['Episodes'] <= 60), 'Series_Length'] = 'Средние (21-60 эп.)'
    df.loc[series_mask & (df['Episodes'] > 60), 'Series_Length'] = 'Длинные (60+ эп.)'

    series_data = df[df['Type'] == 'Series']

all_genres = []
for genres in df['Genre_Clean'].dropna():
    if isinstance(genres, list):
        all_genres.extend([g.strip() for g in genres])

genre_counter = {}
for genre in all_genres:
    genre_counter[genre] = genre_counter.get(genre, 0) + 1

top_genres = dict(sorted(genre_counter.items(), key=lambda x: x[1], reverse=True)[:15])

genre_ratings = {}
for genre in genre_counter.keys():
    mask = df['Genre_Clean'].apply(lambda x: genre in x if isinstance(x, list) else False)
    if mask.sum() > 5:
        genre_ratings[genre] = df.loc[mask, 'Rate'].mean()

top_genres_by_rating = dict(sorted(genre_ratings.items(), key=lambda x: x[1], reverse=True)[:15])

fig1, axes1 = plt.subplots(1, 3, figsize=(18, 6))

axes1[0].barh(list(top_genres.keys()), list(top_genres.values()),
              alpha=0.7, color='steelblue')
axes1[0].set_xlabel('Количество упоминаний')
axes1[0].set_title('Топ-15 жанров по упоминаниям', fontweight='bold')
axes1[0].grid(True, alpha=0.3)

axes1[1].barh(list(top_genres_by_rating.keys()), list(top_genres_by_rating.values()),
              alpha=0.7, color='coral')
axes1[1].set_xlabel('Средний рейтинг IMDB')
axes1[1].set_title('Топ-15 жанров по оценкам', fontweight='bold')
axes1[1].grid(True, alpha=0.3)

axes1[2].hist(df['Rate'], bins=30, edgecolor='black', alpha=0.7, color='skyblue')
axes1[2].axvline(df['Rate'].mean(), color='red', linestyle='--', linewidth=2,
                 label=f'Среднее: {df["Rate"].mean():.2f}')
axes1[2].axvline(df['Rate'].median(), color='green', linestyle='--', linewidth=2,
                 label=f'Медиана: {df["Rate"].median():.2f}')
axes1[2].set_xlabel('Рейтинг IMDB (1-10)')
axes1[2].set_ylabel('Количество')
axes1[2].set_title('Распределение рейтингов', fontweight='bold')
axes1[2].legend()
axes1[2].grid(True, alpha=0.3)

plt.suptitle('АНАЛИЗ ЖАНРОВ И РЕЙТИНГОВ', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()

fig2, axes2 = plt.subplots(1, 3, figsize=(18, 6))

if 'Type' in df.columns:
    type_counts = df['Type'].value_counts()

    for type_name, count in type_counts.items():
        percentage = (count / len(df)) * 100

    colors = ['#ff6b6b', '#4ecdc4'] if len(type_counts) == 2 else None

    axes2[0].pie(type_counts.values, labels=type_counts.index,
                 autopct='%1.1f%%', startangle=90, colors=colors)
    axes2[0].set_title('Соотношение фильмов и сериалов', fontweight='bold')

if 'Series_Length' in df.columns:
    series_data = df[df['Type'] == 'Series']
    if len(series_data) > 0:
        length_counts = series_data['Series_Length'].value_counts()
        length_counts_filtered = length_counts[length_counts.index != 'Not Series']

        if len(length_counts_filtered) > 0:
            for length, count in length_counts_filtered.items():
                percentage = (count / len(series_data)) * 100

            colors = ['#FFD700', '#FFA500', '#8B0000']
            axes2[1].pie(length_counts_filtered.values, labels=length_counts_filtered.index,
                         autopct='%1.1f%%', startangle=90, colors=colors[:len(length_counts_filtered)],
                         textprops={'fontsize': 10})
            axes2[1].set_title('Сериалы по длительности', fontweight='bold')
        else:
            axes2[1].text(0.5, 0.5, 'Нет данных\nо сериалах',
                          ha='center', va='center', fontsize=12)
            axes2[1].set_title('Сериалы по длительности')
    else:
        axes2[1].text(0.5, 0.5, 'Сериалы не найдены',
                      ha='center', va='center', fontsize=12)
        axes2[1].set_title('Сериалы по длительности')


if 'Certificate' in df.columns:
    cert_counts = df['Certificate'].value_counts().head(10)

    for i, (cert, count) in enumerate(cert_counts.head(5).items(), 1):
        percentage = (count / len(df)) * 100

    cert_counts_sorted = cert_counts.sort_values(ascending=True)

    colors = ['lightgreen', 'green', 'yellow', 'orange', 'red',
              'darkred', 'black', 'purple', 'blue', 'cyan']

    axes2[2].barh(cert_counts_sorted.index, cert_counts_sorted.values,
                  alpha=0.7, color=colors[:len(cert_counts_sorted)])
    axes2[2].set_xlabel('Количество записей')
    axes2[2].set_title('Топ возрастных рейтингов', fontweight='bold')
    axes2[2].grid(True, alpha=0.3)

plt.suptitle('АНАЛИЗ ТИПОВ КОНТЕНТА', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()


fig3, axes3 = plt.subplots(1, 3, figsize=(18, 6))

if 'Votes' in df.columns:
    axes3[0].scatter(df['Rate'], np.log10(df['Votes'] + 1),
                     alpha=0.5, s=20, c='purple')
    axes3[0].set_xlabel('Рейтинг IMDB')
    axes3[0].set_ylabel('log10(Количество голосов + 1)')
    axes3[0].set_title('Рейтинг vs Популярность', fontweight='bold')
    axes3[0].grid(True, alpha=0.3)

    z = np.polyfit(df['Rate'], np.log10(df['Votes'] + 1), 1)
    p = np.poly1d(z)
    axes3[0].plot(sorted(df['Rate']), p(sorted(df['Rate'])), "r--", alpha=0.8, linewidth=2)


top_content = df.nlargest(8, 'Rate')
top_names = top_content['Name'].tolist()
top_ratings = top_content['Rate'].tolist()
top_types = top_content['Type'].tolist()


colors = []
for t in top_types:
    if t == 'Film':
        colors.append('#ff6b6b')
    else:
        colors.append('#4ecdc4')

y_pos = np.arange(len(top_names))
axes3[1].barh(y_pos, top_ratings, alpha=0.7, color=colors)
axes3[1].set_yticks(y_pos)
axes3[1].set_yticklabels([name[:20] + "..." if len(name) > 20 else name for name in top_names], fontsize=9)
axes3[1].set_xlabel('Рейтинг IMDB')
axes3[1].set_title('Топ контента по рейтингу', fontweight='bold')
axes3[1].grid(True, alpha=0.3)


for i, (rating, type_name) in enumerate(zip(top_ratings, top_types)):
    axes3[1].text(rating + 0.02, i, type_name, va='center', fontsize=9, color='black', fontweight='bold')


if 'Certificate' in df.columns:
    cert_rating_stats = df.groupby('Certificate').agg({
        'Rate': ['mean', 'count'],
    }).round(3)

    cert_rating_filtered = cert_rating_stats[cert_rating_stats[('Rate', 'count')] > 5]

    if len(cert_rating_filtered) > 0:
        cert_rating_sorted = cert_rating_filtered.sort_values(by=('Rate', 'mean'), ascending=False)

        top_cert_ratings = cert_rating_sorted.head(8)

        certs = top_cert_ratings.index
        ratings = top_cert_ratings[('Rate', 'mean')]

        bars = axes3[2].bar(certs, ratings, alpha=0.7, color=['green', 'lightgreen', 'yellow',
                                                              'orange', 'red', 'darkred', 'brown', 'black'])
        axes3[2].set_xlabel('Возрастной рейтинг')
        axes3[2].set_ylabel('Средний рейтинг IMDB')
        axes3[2].set_title('Возрастные рейтинги по оценкам', fontweight='bold')
        axes3[2].tick_params(axis='x', rotation=45)
        axes3[2].grid(True, alpha=0.3)

        for bar, rating in zip(bars, ratings):
            height = bar.get_height()
            axes3[2].text(bar.get_x() + bar.get_width() / 2., height + 0.05,
                          f'{rating:.2f}', ha='center', va='bottom', fontsize=9)

plt.suptitle('ДОПОЛНИТЕЛЬНЫЙ АНАЛИЗ', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.show()


print(f"Всего записей: {len(df):,}")
print(f"Средний рейтинг: {df['Rate'].mean():.2f}")
print(f"Медианный рейтинг: {df['Rate'].median():.2f}")
print(f"Минимальный рейтинг: {df['Rate'].min():.2f}")
print(f"Максимальный рейтинг: {df['Rate'].max():.2f}")


if 'Type' in df.columns:
    type_stats = df['Type'].value_counts()
    for type_name, count in type_stats.items():
        percentage = (count / len(df)) * 100
        print(f"{type_name}: {count} ({percentage:.1f}%)")

