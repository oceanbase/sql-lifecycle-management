# SQL-Lifecycle-Management API Style Guide

SQL-Lifecycle-Management uses REST API and this doc describes the corresponding API style guide.

The guiding principal for our style guide is **consistency**.

# Methods

## Prefer PATCH over PUT

Most of the time, we only want to do partial update on the resource, and we should use PATCH accordingly. PUT on the other hand means to overwrite the entire resource with the request fields and would more likely to reset existing fields unexpectedly.

# Resource URL naming

## Use database_alias +(string concatenation) user_id for addressing the specific resource

SQL-Lifecycle-Management uses database_alias +(string concatenation) user_id as the uniquely identifies for database resources. To address a database resource, we use GET `/database/:database_alias`, Then get user_id by authentication or parameter passing.

## Use lower case, kebab-case for phrases

Use `/foo/bar-baz` instead of `/foo/barBaz` or `/foo/barbaz`

## Use singular form even for collection resource

Use `GET /database` instead of `GET /databases` to fetch the list of issues.

## Use a separate `/{{resource}}/batch` for batch operation

If the resource supports batch operation, then use a separate `/batch` endpoint under that resource.

# Messages

## Property Name Convention

We use json messages to communicate between backend and frontend following [Google JSON Style Guide](https://google.github.io/styleguide/jsoncstyleguide.xml). Property names must be camelCased, ascii strings.

# Misc

1. Timestamps should be Unix timestamp (UTC timezone) in seconds whenever possible. The names should be in the format of `xxTs` such as `createdTs`. Timestamps that need precision should be nanoseconds, e.g. perf profiling. The names should be in the format of `xxNs`.

# References

1. [Bytebase API Style Guide](https://github.com/bytebase/bytebase/blob/main/docs/api-style-guide.md)