# Flusher
It helps flush your models, files, datas e t c where ever you wants in async mode by packages of data 

## How To

- Implement or get your own type that shall be flushed by package queue
```
// For example FlushItem
type FlushItem struct {
	// Name
	Name string
	// Number
	Number int
}
```

- You need to implement your own flush method according to signature
```
func(block []*T) (e porterr.IError)
// for example flush operation can take 5 second
func testFlusher(block []*FlushItem) (e porterr.IError) {
	time.Sleep(time.Second * 5)
	fmt.Println(len(block), "items flushed")
	return
}
```

- Use FlushQueue to flash your data
```
// Init FlushQueue struct with 10 items package size and testFlusher func implemented above
fq := NewFlushQueue[FlushItem](10, testFlusher)

// Async idle and flush for 4 worker and 0 = DefaultFlushPeriod (0.5 second)
go fq.Idle(4, 0)

// Just add items to queue
for i := 0; i < 100; i++ {
    fq.AddItem(getTestItem())
}
```

#### Enjoy the flusher

#### If you find this package useful or want to support the author, you can send tokens to any of these wallets
- Bitcoin: bc1qgx5c3n7q26qv0tngculjz0g78u6mzavy2vg3tf
- Ethereum: 0x62812cb089E0df31347ca32A1610019537bbFe0D
- Dogecoin: DET7fbNzZftp4sGRrBehfVRoi97RiPKajV
