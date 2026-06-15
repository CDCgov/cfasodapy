# cfasodapy

See the [API reference](api.md) for specifics.

## Getting an app token

1. Navigate to <https://evergreen.data.socrata.com/>
2. Create an account and sign in
3. Under Developer Settings, create an App Token

See the Socrata docs on [app tokens](https://dev.socrata.com/docs/app-tokens.html) and [how to obtain one](https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys).

## Writing queries

The Socrata docs can be hard to navigate. The key pages are:

- [Queries using SODA3](https://dev.socrata.com/docs/queries/): Describes the "options" in the query, including the SoQL query string.
- [The query Option](https://dev.socrata.com/docs/queries/query): The syntax for the SoQL query string.
- [SoQL Function and Keyword Listing](https://dev.socrata.com/docs/functions/)
- [Data Transform Listing](https://dev.socrata.com/docs/transforms/)

Confusingly, the docs use term "query" to refer to the data passed to the API endpoint as well as to the query _string_ in that data.

## Working with polars

cfasodapy returns data as a list of dictionary records, which can be read into a [polars](https://docs.pola.rs/) dataframe with [`polars.from_dicts()`](https://docs.pola.rs/api/python/dev/reference/api/polars.from_dicts.html).
