package com.eecs476;

import java.util.ArrayList;
import java.util.List;

public class Combination {
    public static List<List<String>> combinator(List<String> input, int passNum) {
        List<List<String>> res = new ArrayList<>();
        List<String> temp = new ArrayList<>();
        combinator_helper(input, input.size(), passNum, 0, temp, 0, res);
        return res;
    }
    // n = input.size()
    // r = passNum
    // index = current index in data
    // data = temporary array to store current combination
    // i = index of current element in input[]
    private static void combinator_helper (List<String> input, int n, int r, int index, List<String> data, int i, List<List<String>> res) {

        if (index == r) {
            List<String> temporary = new ArrayList<>(data);
            res.add(temporary);
            return;
        }
        if (i >= n) {
            return;
        }
        data.add(input.get(i));
        combinator_helper(input, n, r, index+1, data, i+1, res);

        data.remove(data.size()-1);
        combinator_helper(input, n, r, index, data, i+1, res);


    }

}
