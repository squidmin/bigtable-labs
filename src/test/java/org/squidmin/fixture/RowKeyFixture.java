package org.squidmin.fixture;


import lombok.*;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RowKeyFixture {

    @NonNull
    private String foo;
    @NonNull
    private String bar;

    @Override
    public String toString() {
        return foo + "#" + bar;
    }

}
