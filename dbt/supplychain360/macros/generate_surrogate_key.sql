{% macro generate_surrogate_key(field_list) %}

    {# Initializing the MD5 hash with a concatenation of the fields #}
    md5(
        {% for field in field_list %}
            coalesce(cast({{ field }} as varchar), '_null_')
            {% if not loop.last %} || '-' || {% endif %}
        {% endfor %}
    )

{% endmacro %}