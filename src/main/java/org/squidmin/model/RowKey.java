package org.squidmin.model;


import lombok.*;


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RowKey {

    @NonNull
    private String segmentA;
    @NonNull
    private String segmentB;
    @NonNull
    private String segmentC;

    public static String delim = "#";

    @Override
    public String toString() {
        return
            segmentA + delim +
            segmentB + delim +
            segmentC;
    }

}
