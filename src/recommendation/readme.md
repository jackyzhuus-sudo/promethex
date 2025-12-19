# Recommendation Service
Embedding everthing
```proto
service RecommendationHttp {

  //Embedding all the data with patterns, the total count of embedded data will be len(pattern) * len(data). for more information, please refer to [example.md](./example.md)
  rpc Embedding(EmbeddingRequest) returns (EmbeddingResponse) {}


  // return top K ids, will match the type first, then word. the count of result will be len(word) * k.
  rpc Hot(HotRequest) returns (HotResponse) {}
}
```
### How to use
```bash
pip install -r requirements.txt
python main.py
```


1. Embedding your data
2. Query the top K of your data



[Reference](./doc/docs.md)
[Code](./test.py)

