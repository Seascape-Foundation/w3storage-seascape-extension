
# `database` extension reference
All `database` extensions are sharing the
same command interfaces. The reason is to make all storages
compatible with each other. So service developers
can replace the storage for the better solution,
or to enable multiple storages.

For example, user may want to have two databases.
One for the long term storage using `SQL` based database.
And another one to store for the cache based on `redis`.

To create the `database` kind of extensions, your
service has to met two requirements:
*configuration* and *commands*.

### `database `configuration
> The services implement set the configuration in `service.yml`.

The `database` extension requires the following
parameters to be set in `service.yml`:

```yaml
Services:
  - Type: Extension
    Name: database
```

> The top parameter should not be changed. Only other
> parameters are editable.

### `database` commands
The following commands should be enabled in the
extension for the end users:

| Command | Reply Parametes               | Description                                         |
|---------|-------------------------------|-----------------------------------------------------|
|`"select-row"`| `outputs: key_value.KeyValue` | Get one row, if it doesn't exist, return error      |
|`"select"` | `rows: key_value.KeyValue[]`  | Read multiple line                                  |                            |
|`"insert"` | `id: string`                  | insert new data                                     |
|`"update"` | `id: string`                  | update the existing row                             |
| `"exist"` | `exist: boolean`              | returns true or false if select query has some rows |
| `"delete"` | ``                            |  delete some rows from database                     |

The commands have the same request parameters:

```typescript
{
    tables:  Array<string>;
	fields:  Array<string>;
	where:   string;
	arguments: Array<any>;
}
```

The only `tables` parameter is required. The rest of the
request parameters are on the extension or the command type.
