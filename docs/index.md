# cfasodapy

See the [API reference](api.md) for specifics.

## Getting an app token

1. Navigate to <https://evergreen.data.socrata.com/>
2. Create an account and sign in
3. Under Developer Settings, create an App Token

See the [Socrata docs](https://dev.socrata.com/docs/app-tokens.html) on app tokens.

## Working with polars

cfasodapy returns data as a list of dictionary records, which can be read into a [polars](https://docs.pola.rs/) dataframe with [`polars.from_dicts()`](https://docs.pola.rs/api/python/dev/reference/api/polars.from_dicts.html).
