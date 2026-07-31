/*
    Coerencia de candle: maxima >= abertura e fechamento, minima <= abertura e
    fechamento, maxima >= minima.

    E o teste que pega erro de agregacao (min_by/max_by trocados, janela errada)
    antes de o grafico mentir para quem esta olhando.
*/

select
    ticker,
    window_start,
    open_price,
    high_price,
    low_price,
    close_price
from {{ ref('gold_intraday_candles') }}
where high_price < low_price
   or high_price < greatest(open_price, close_price)
   or low_price  > least(open_price, close_price)
