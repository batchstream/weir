| Experiment | Workers | Native / Weir ops/s | Weir/native | Business commands/op N / W | Estimated DB CPU s/1,000 ops N / W |
| --- | ---: | ---: | ---: | ---: | ---: |
| mongo-candidate-mixed | 32 | 36,644 / 12,386 | 0.34 | 1.0000 / 0.6009 | 0.0792 / 0.0547 |
| mongo-candidate-mixed | 256 | 39,667 / 29,961 | 0.76 | 1.0000 / 0.0637 | 0.0968 / 0.0149 |
| mongo-candidate-mixed-wait1ms | 32 | 36,457 / 18,439 | 0.51 | 1.0000 / 0.5552 | 0.0792 / 0.0588 |
| mongo-candidate-mixed-wait1ms | 256 | 38,669 / 30,044 | 0.78 | 1.0000 / 0.0686 | 0.0981 / 0.0171 |
| mongo-candidate-read | 32 | 44,412 / 22,832 | 0.51 | 1.0000 / 0.4076 | 0.0789 / 0.0377 |
| mongo-candidate-read | 256 | 42,829 / 34,507 | 0.81 | 1.0000 / 0.0551 | 0.0825 / 0.0084 |
| mongo-candidate-write | 32 | 3,611 / 1,040 | 0.29 | 1.0000 / 0.0312 | 0.2816 / 0.1038 |
| mongo-candidate-write | 256 | 20,288 / 18,610 | 0.92 | 1.0000 / 0.0078 | 0.2385 / 0.0417 |
| mongo-candidate-write-wait1ms | 32 | 3,632 / 3,686 | 1.01 | 1.0000 / 0.0649 | 0.2784 / 0.0737 |
| mongo-candidate-write-wait1ms | 256 | 21,100 / 20,356 | 0.96 | 1.0000 / 0.0254 | 0.2409 / 0.0629 |
| search-candidate-read | 32 | 8,669 / 4,142 | 0.48 | - / - | 0.1153 / 0.2391 |
| search-candidate-read | 256 | 12,629 / 18,479 | 1.46 | - / - | 0.0798 / 0.0323 |
| search-candidate-write | 32 | 1,362 / 1,135 | 0.83 | - / - | 0.3789 / 0.2581 |
| search-candidate-write | 256 | 1,459 / 19,061 | 13.06 | - / - | 0.2122 / 0.0275 |
