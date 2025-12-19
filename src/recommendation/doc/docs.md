# Protocol Documentation

<a name="top"></a>

## Table of Contents

- [recommendation.proto](#recommendation-proto)

  - [EmbeddingRequest](#recommendation-v1-EmbeddingRequest)
  - [EmbeddingResponse](#recommendation-v1-EmbeddingResponse)
  - [EmbeddingResponse.EmbeddingResult](#recommendation-v1-EmbeddingResponse-EmbeddingResult)
  - [HotRequest](#recommendation-v1-HotRequest)
  - [HotResponse](#recommendation-v1-HotResponse)
  - [HotResponse.HotResult](#recommendation-v1-HotResponse-HotResult)

  - [RecommendationHttp](#recommendation-v1-RecommendationHttp)

- [Scalar Value Types](#scalar-value-types)

<a name="recommendation-proto"></a>

<p align="right"><a href="#top">Top</a></p>

## recommendation.proto

<a name="recommendation-v1-EmbeddingRequest"></a>

### EmbeddingRequest

| Field   | Type                                        | Label    | Description                                                                                                                                                                                                                    |
| ------- | ------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| pattern | [string](#string)                           | repeated | the pattern while embedding, you should tag some of fieldNames from the data with `@`, E.g `@title@`,`@fieldName@`. you can combine any format that you want. for more information, please refer to [example.md](./example.md) |
| data    | [google.protobuf.Any](#google-protobuf-Any) | repeated | the data that you want to embed, can be anything. but must have a field `id`                                                                                                                                                   |

<a name="recommendation-v1-EmbeddingResponse"></a>

### EmbeddingResponse

| Field | Type                                                                                      | Label    | Description         |
| ----- | ----------------------------------------------------------------------------------------- | -------- | ------------------- |
| res   | [EmbeddingResponse.EmbeddingResult](#recommendation-v1-EmbeddingResponse-EmbeddingResult) | repeated | embedded data count |

<a name="recommendation-v1-EmbeddingResponse-EmbeddingResult"></a>

### EmbeddingResponse.EmbeddingResult

| Field     | Type              | Label    | Description        |
| --------- | ----------------- | -------- | ------------------ |
| id        | [string](#string) |          | the id of the data |
| pattern   | [string](#string) |          |                    |
| embedding | [float](#float)   | repeated |                    |

<a name="recommendation-v1-HotRequest"></a>

### HotRequest

| Field   | Type              | Label    | Description                      |
| ------- | ----------------- | -------- | -------------------------------- |
| word    | [string](#string) | repeated | the words that you want to query |
| k       | [int32](#int32)   |          | how many ids that you want       |
| pattern | [string](#string) |          | the type of the data             |

<a name="recommendation-v1-HotResponse"></a>

### HotResponse

| Field | Type                                                              | Label    | Description                    |
| ----- | ----------------------------------------------------------------- | -------- | ------------------------------ |
| res   | [HotResponse.HotResult](#recommendation-v1-HotResponse-HotResult) | repeated | the result of the hot response |

<a name="recommendation-v1-HotResponse-HotResult"></a>

### HotResponse.HotResult

| Field | Type              | Label    | Description              |
| ----- | ----------------- | -------- | ------------------------ |
| word  | [string](#string) |          | the word that you passed |
| count | [int64](#int64)   |          | the count of the ids     |
| ids   | [string](#string) | repeated | the ids matched          |

<a name="recommendation-v1-RecommendationHttp"></a>

### RecommendationHttp

| Method Name | Request Type                                            | Response Type                                             | Description                                                                                                                                                                    |
| ----------- | ------------------------------------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Embedding   | [EmbeddingRequest](#recommendation-v1-EmbeddingRequest) | [EmbeddingResponse](#recommendation-v1-EmbeddingResponse) | Embedding all the data with patterns, the total count of embedded data will be **len(pattern) \* len(data)**. for more information, please refer to [example.md](./example.md) |
| Hot         | [HotRequest](#recommendation-v1-HotRequest)             | [HotResponse](#recommendation-v1-HotResponse)             | return top K ids, will match the type first, then word. the count of result will be **len(word) \* k**.                                                                        |

## Scalar Value Types

| .proto Type                    | Notes                                                                                                                                           | C++    | Java       | Python      | Go      | C#         | PHP            | Ruby                           |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---------- | ----------- | ------- | ---------- | -------------- | ------------------------------ |
| <a name="double" /> double     |                                                                                                                                                 | double | double     | float       | float64 | double     | float          | Float                          |
| <a name="float" /> float       |                                                                                                                                                 | float  | float      | float       | float32 | float      | float          | Float                          |
| <a name="int32" /> int32       | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32  | int        | int         | int32   | int        | integer        | Bignum or Fixnum (as required) |
| <a name="int64" /> int64       | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64  | long       | int/long    | int64   | long       | integer/string | Bignum                         |
| <a name="uint32" /> uint32     | Uses variable-length encoding.                                                                                                                  | uint32 | int        | int/long    | uint32  | uint       | integer        | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64     | Uses variable-length encoding.                                                                                                                  | uint64 | long       | int/long    | uint64  | ulong      | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32     | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s.                            | int32  | int        | int         | int32   | int        | integer        | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64     | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s.                            | int64  | long       | int/long    | int64   | long       | integer/string | Bignum                         |
| <a name="fixed32" /> fixed32   | Always four bytes. More efficient than uint32 if values are often greater than 2^28.                                                            | uint32 | int        | int         | uint32  | uint       | integer        | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64   | Always eight bytes. More efficient than uint64 if values are often greater than 2^56.                                                           | uint64 | long       | int/long    | uint64  | ulong      | integer/string | Bignum                         |
| <a name="sfixed32" /> sfixed32 | Always four bytes.                                                                                                                              | int32  | int        | int         | int32   | int        | integer        | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes.                                                                                                                             | int64  | long       | int/long    | int64   | long       | integer/string | Bignum                         |
| <a name="bool" /> bool         |                                                                                                                                                 | bool   | boolean    | boolean     | bool    | bool       | boolean        | TrueClass/FalseClass           |
| <a name="string" /> string     | A string must always contain UTF-8 encoded or 7-bit ASCII text.                                                                                 | string | String     | str/unicode | string  | string     | string         | String (UTF-8)                 |
| <a name="bytes" /> bytes       | May contain any arbitrary sequence of bytes.                                                                                                    | string | ByteString | str         | []byte  | ByteString | string         | String (ASCII-8BIT)            |
