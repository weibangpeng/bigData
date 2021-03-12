package com.wbp;


public class LeetCodeTest {
    public int[][] findContinuousSequence(int target) {
        int start = 1;
        int end = 1;
        int sum = 0;
        for(int i =1 ;i< target;i++){
            sum = getSum(start,end);
            if(target == sum){
                System.out.println("start"+start);
                System.out.println("end"+end);
                end+=1;
            }else if(target > sum) {
                end+=1;
            }else if(target < sum){
                start+=1;
            }

        }
        return new int[0][0];
    }

    public  int getSum(int start,int end){
        int sum = 0;
        if(start == end){
            return 0;
        }
        for(int i= start;start<=end;i++){
            sum +=i;
        }
        return sum;
    }

    public static void main(String[] args) {
        LeetCodeTest  test= new LeetCodeTest();
        test.findContinuousSequence(9);
    }
}