# Embedding

### Input

```javascript
const data = [
  {
    "id':"id from your database",
    "title":"this is title",
    "description":"this is description",
    "rules":"this is rules"
    "type":"this is type"
  },
  {
    "id":"another id from your database",
    "title":"this is aother title",
    "description":"this is aother description",
    "rules":"this is aother rules"
    "type":"this is aother type"
  }
]
const pattern=[
  "@title@ : @description@",
  "@title@ : @rules@",
  "@type@ : @title@",
  "@title@ | @description@ | @rules@",
]
```

##### Embedding the pattern first (will be used in HOT)

```javascript
dataEmbedding=[
  "@title@ : @description@",
  "@title@ : @rules@",
  "@type@ : @title@",
  "@title@ | @description@ | @rules@",
]

<...embedding logic...>

dataEmbedded=[
  {"id":"@title@ : @description@","embedding":[...]},
  {"id":"@title@ : @rules@","embedding":[...]},
  {"id":"@type@ : @title@","embedding":[...]},
  {"id":"@title@ | @description@ | @rules@","embedding":[...]},
]

<...store data into database...>
```

##### Embedding the data with patterns

```javascript
--------- first embedding ---------
dataEmbedding=[
  "this is title : this is description",
  "this is another title : this is another description",
]
...embedding...
dataEmbedded=[
  {"id":"id from your database","pattern":"@title@ : @description@","embedding":[...]},
  {"id":"another id from your database","pattern":"@title@ : @description@","embedding":[...]},
]
<...store data into database...>


--------- second embedding ---------
dataEmbedding2=[
  "this is title : this is rules",
  "this is another title : this is another rules",
]
...embedding...
dataEmbedded2=[
  {"id":"id from your database","pattern":"@title@ : @rules@","embedding":[...]},
  {"id":"another id from your database","pattern":"@title@ : @rules@","embedding":[...]},
]
<...store data into database...>

<...embedding the rest of data...>

```

# Hot

```javascript
msg = {
  pattern: "title",
  k: 10,
  word: ["ethereum", "bitcoin"],
};
```

1. Get top 1 embedded pattern with `pattern` from all patterns.
2. Get top k embedded ids with `word` from all `pattern` embedded data
