package org.squidmin.model;


import com.google.cloud.bigtable.data.v2.models.Row;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BigtableRowWrapper {

    private int index;
    private Row value;

}
