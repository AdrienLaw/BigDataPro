package com.adrien.datastream;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class DataEntity {
    String id;
    Long timestemp;
    Long value;
}
