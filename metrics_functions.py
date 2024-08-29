import pandas as pd
import pytz # устаревший модуль, в более новых версиях заменён на zoneinfo
from pytz.exceptions import UnknownTimeZoneError
from datetime import timedelta


def retention_rate(
    n, 
    path_to_file="https://getfile.dokpub.com/yandex/get/https://disk.yandex.com/d/WqlBuzCFovm-3g",
    timezone="Europe/Berlin",
    limit=0,
    start_cohort=None, 
    end_cohort=None,
    bootstrap_size=10000,
    bootstrap_ci=0.95
):
    '''
    Функция рассчитывает retention n-ного дня. Пользователи разбиваются на когорты по дням регистрации 
        (точно соответствующим дням первого входа в игру, поэтому считываем только файл со входами в игру).
    
    Аргументы:
        n - день, retention на который мы считаем, должен быть целым числом (желательно положительным);
        path_to_file - путь к файлу, где хранятся данные о всех входах пользователей в игру;
            это должен быть csv-файл с разделителем ";", состоящий из 2 столбцов: 'auth_ts' со временем 
            входа в игру (в форме UNIX-таймстемпа) и 'uid' с идентификаторами пользователей;
        timezone - существующий часовой пояс в виде строки;
        limit - нижний предел численности когорт (если мы считаем целесообразным брать для расчётов
            только когорты выше определённой численности);
        start_cohort - начальная когорта периода, за который мы считаем retention (формат ввода 'YYYY-MM-DD');
        end_cohort - конечная когорта периода, за который мы считаем retention (формат ввода 'YYYY-MM-DD');
        bootstrap_size - число подвыборок, которые мы берём при вычислении доверительного интервала для
            retention;
        bootstrap_ci - уровень доверия, с которым мы определяем retention.
    '''
    
    # Стартовые проверки на осмысленность входных значений аргументов.
    if not isinstance(n, int):
        print('Ошибка. n должно быть целым числом.\n')
        return
    
    elif n < 0:
        print('Нет большого смысла в отрицательном n. Нельзя войти в игру раньше первого входа.\n\
Но если хотите, можем посчитать.\n')
        
    elif n == 0:
        print('Нет большого смысла в n=0. Retention нулевого дня всегда 100%.\n\
Но если хотите, можем посчитать.\n')
        
    if not isinstance(bootstrap_size, int):
        print('Ошибка. bootstrap_size должно быть целым числом.\n')
        return
    
    elif bootstrap_size <= 0:
        print('Ошибка. bootstrap_size должно быть положительным числом.\n')
        return
    
    if not isinstance(bootstrap_ci, (int, float)):
        print('Ошибка. bootstrap_ci должно быть числом.\n')
        return
    
    elif not (0 <= bootstrap_ci <= 1):
        print('Ошибка. bootstrap_ci должно быть числом от 0 до 1.\n')
        return
        
    if not isinstance(limit, (int, float)):
        print('Ошибка. Предел численности когорт должен быть числом.\n')
        return
        
    if limit < 0:
        print('Нет большого смысла в отрицательном пределе численности когорт.\n\
Этот показатель не может быть меньше 0.\n\
Но если хотите, можем посчитать.\n')
    
    if not timezone in pytz.all_timezones:
        print('Ошибка. Неизвестная таймзона.\n')
        return
    
    if not start_cohort == None:
        try:
            datetime.strptime(start_cohort, '%Y-%m-%d')
    
        except Exception:
            print("Ошибка. Начальная когорта для расчётов должна быть заведена в формате 'YYYY-MM-DD'\n\
и должна являться валидной датой.\n")
            return
        
    if not end_cohort == None:
        try:
            datetime.strptime(end_cohort, '%Y-%m-%d')
    
        except Exception:
            print("Ошибка. Конечная когорта для расчётов должна быть заведена в формате 'YYYY-MM-DD'\n\
и должна являться валидной датой.\n")
            return
    
    try:
        
        # Считываем данные.
        authorization_data = pd.read_csv(path_to_file, sep=';') 

        # Переводим UNIX-таймстемпы в наше локальное время.
        authorization_data['auth_date'] = pd.to_datetime(authorization_data['auth_ts'], unit='s') \
            .dt.tz_localize('UTC') \
            .dt.tz_convert(timezone) 

        # Если период вычислений не задан, берём в этом качестве самую первую и самую последнюю запись.
        if start_cohort == None:
            start_cohort_dt = authorization_data['auth_date'].min().date()
        else:
            start_cohort_dt = pd.to_datetime(start_cohort).date()

        if end_cohort == None:
            end_cohort_dt = authorization_data['auth_date'].max().date()
        else:
            end_cohort_dt = pd.to_datetime(end_cohort).date()

        # Определяем время первого входа в игру для каждого пользователя.
        first_auth = authorization_data \
            .groupby('uid', as_index=False) \
            .auth_date \
            .min() \
            .rename(columns={'auth_date': 'first_auth'})

        # Отфильтровываем период для расчёта (если нужно).
        if (
               start_cohort_dt != authorization_data['auth_date'].min().date() 
            or end_cohort_dt != authorization_data['auth_date'].max().date()
        ):
            first_auth = first_auth.loc[
                    (first_auth.first_auth.dt.date >= start_cohort_dt) 
                & (first_auth.first_auth.dt.date <= end_cohort_dt)
            ]

        # Разобьём пользователей на когорты по дням. 
        # Для производительности будем хранить ID пользователей во множествах. 
        first_auth_by_day_users = first_auth \
            .resample(rule='D', on='first_auth') \
            .agg({'uid': set}) \
            .reset_index() \
            .rename(columns={'first_auth': 'cohort', 'uid': 'users_set'})

        # Посчитаем количество уникальных пользователей в каждой когорте.
        first_auth_by_day_users['users'] = first_auth_by_day_users['users_set'].apply(len)

        # Отфильтруем слишком маленькие когорты (если нужно).
        if limit > 0:
            first_auth_by_day_users = first_auth_by_day_users.loc[first_auth_by_day_users.users >= limit]

        # Для удобства вычислений переведём формат данных в дни.
        first_auth_by_day_users['cohort'] = first_auth_by_day_users['cohort'].dt.tz_localize(None)

        # Посчитаем для каждой когорты n-ный день (тот, на который мы определяем retention).
        first_auth_by_day_users['n_day'] = first_auth_by_day_users['cohort'] + timedelta(days=n)

        # Посмотрим, какие пользователи заходили в игру в каждый из дней.
        # Использование множеств обеспечит уникальность.
        auth_by_day_users = authorization_data \
            .resample(rule='D', on='auth_date') \
            .agg({'uid': set}) \
            .reset_index() \
            .rename(columns={'auth_date': 'n_day', 'uid': 'n_day_users_set'})

        # Также переведём формат данных в дни.
        auth_by_day_users['n_day'] = auth_by_day_users['n_day'].dt.tz_localize(None)

        # Соединим данные о когортах с данными о посещениях в n-ый день.
        # Когорты, где n-ный день ещё не настал и не попал в данные, придётся дропнуть.
        merged_data = first_auth_by_day_users \
            .merge(auth_by_day_users, on='n_day', how='left') \
            .dropna()

        # Посчитаем, сколько уникальных пользователей из каждой когорты заходили в игру на n-ный день.
        merged_data['recurrent_n_day_users'] = merged_data \
            .apply(lambda x: len(x['users_set'].intersection(x['n_day_users_set'])), axis=1)

        # Теперь у нас есть все данные для расчёта retention.
        RR = merged_data.recurrent_n_day_users.sum() / merged_data.users.sum()
        
        # Определим доверительный интервал для retention. Наши данные могут оказаться распределены 
        # любым образом и не подходить для применения обычных (как параметрических, так и непараметрических)
        # статистических тестов. Поэтому используем бутстрап.
        RR_list = []
        merged_data_subset = merged_data[['users', 'recurrent_n_day_users']] # Оставим только нужное.
        
        # Возьмём подвыборки с повторением из нашего датафрейма для расчёта retention
        # и получим серию значений retention.
        for i in range(bootstrap_size):
            sampled_df = merged_data_subset.sample(frac=1, replace=True)
            sampled_RR = sampled_df.recurrent_n_day_users.sum() / sampled_df.users.sum()
            RR_list.append(sampled_RR)
        
        RR_series = pd.Series(RR_list)
        
        # Из этой серии расчитаем границы доверительного интервала и бутстрапированное среднее.
        ci_low = RR_series.quantile(q=(1-bootstrap_ci)/2) # Нижняя граница ДИ. 
        ci_high = RR_series.quantile(q=1-(1-bootstrap_ci)/2) # Верхняя граница ДИ.
        bootstrap_mean = RR_series.mean() # Бутстрапированное среднее.
        
        return RR, ci_low, ci_high, bootstrap_mean
        
    except ValueError:
        
        print('Ошибка. Нет данных за требуемый промежуток времени;\n\
либо слишком высокий предел численности когорты.\n\n\
И проверьте, что за файл Вы подаёте на вход.\n')
        
        return
    
    except FileNotFoundError:
        
        print('Ошибка. По указанному адресу файл не найден.\n')
        
        return
    
    except KeyError:
        
        print('Ошибка. Заголовки столбцов не те. Это точно нужный файл?\n')
        
        return
    
    except Exception:
        
        print('Ошибка. Неизвестная ошибка.\n')
        
        return
        