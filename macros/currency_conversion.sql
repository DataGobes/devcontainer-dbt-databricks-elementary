{%- macro convert_lc_to_eur(lc_column, fxrate_column, precision=2) -%}
    {{ lc_column }} as {{ lc_column }}_lc,
        round({{ lc_column }} / {{ fxrate_column }}, {{ precision }}) as {{ lc_column }}_eur,
{%- endmacro -%}

-- Adaption of the convert_lc_to_eur macro to allow for flexibility in the column naming
-- Used for the GA4 models 
-- Input lc_column: field to convert, fxrate:rate to use for conversion
{%- macro convert_to_eur(lc_column, fxrate, precision=2) -%}
        round({{ lc_column }} / {{ fxrate }}, {{ precision }})
{%- endmacro -%}