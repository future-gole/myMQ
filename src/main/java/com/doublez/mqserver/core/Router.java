package com.doublez.mqserver.core;

import com.doublez.common.MqException;

/**
 * 实现交换机转发规则
 * 验证bindingKey是否合法
 */
public class Router {
    //bindingKey 构造规则
    //1. 数字，字母，下划线
    //2. 使用 . 分割
    //3. 允许使用# 和 * 作为通配符，但是通配符只能是单独的分段
    public boolean checkBindingKey(String bindingKey) {
        // 1. 规则：空字符串通常被认为是无效的路由键
        if (bindingKey == null || bindingKey.isEmpty()) {
            //todo
            return true;
        }

        // 2. 规则：不允许以 '.' 开头或结尾，也不允许出现连续的 '..' (空分段)
        if (bindingKey.startsWith(".") || bindingKey.endsWith(".")) {
            return false;
        }
        if (bindingKey.contains("..")) {
            return false;
        }

        // 3. 使用 '.' 分割字符串为各个分段
        // 注意：split 方法的参数是正则表达式，所以 '.' 需要转义为 "\\."
        String[] segments = bindingKey.split("\\.");

        // 4. 验证每个分段
        for (String segment : segments) {
            // 分割后不应该出现空分段，此检查主要用于防御性编程，
            // 因为前面的 `contains("..")` 已经处理了这种情况。
            if (segment.isEmpty()) {
                return false;
            }

            // 规则 3：检查分段是否为单独的通配符
            if (segment.equals("*") || segment.equals("#")) {
                continue; // 是合法的通配符分段
            }

            // 规则 1：检查分段是否只包含数字、字母、下划线
            for (char ch : segment.toCharArray()) {
                if (!((ch >= 'a' && ch <= 'z') ||
                        (ch >= 'A' && ch <= 'Z') ||
                        (ch >= '0' && ch <= '9') ||
                        (ch == '_'))) {
                    // 如果分段中包含除了数字、字母、下划线之外的字符，则不合法
                    // 这也包括了通配符没有单独成段的情况（如 "foo*bar"）
                    return false;
                }
            }
        }

        // 所有分段都合法，则整个 bindingKey 合法
        return true;
    }
    //routingKey 构造规则
    //1. 数字，字母，下划线
    //2. 使用 . 分割
    public boolean checkRoutingKey(String routingKey) {
        if (routingKey == null || routingKey.isEmpty()) {
            //todo
            return true;
        }

        // 2. 规则：不允许以 '.' 开头或结尾，也不允许出现连续的 '..' (空分段)
        // 路由键通常不允许以点开始或结束，也不允许有空的分段
        if (routingKey.startsWith(".") || routingKey.endsWith(".")) {
            return false;
        }
        if (routingKey.contains("..")) {
            return false;
        }

        // 3. 使用 '.' 分割字符串为各个分段
        // 注意：split 方法的参数是正则表达式，所以 '.' 需要转义为 "\\."
        String[] segments = routingKey.split("\\.");

        // 4. 验证每个分段
        for (String segment : segments) {
            // 分割后不应该出现空分段，此检查主要用于防御性编程，
            // 因为前面的 `contains("..")` 已经处理了这种情况。
            if (segment.isEmpty()) {
                return false;
            }

            // 规则 1：检查分段是否只包含数字、字母、下划线
            for (char ch : segment.toCharArray()) {
                if (!((ch >= 'a' && ch <= 'z') ||
                        (ch >= 'A' && ch <= 'Z') ||
                        (ch >= '0' && ch <= '9') ||
                        (ch == '_'))) {
                    // 如果分段中包含除了数字、字母、下划线之外的字符，则不合法
                    return false;
                }
            }
        }

        // 所有分段都合法，则整个 routingKey 合法
        return true;
    }

    public boolean route(ExchangeType type, Binding binding, Message message) throws MqException {
        if(type == ExchangeType.FANOUT) {
            return true;
        }else if(type == ExchangeType.TOPIC) {

            return routeTopic(binding,message);
        }else {
            throw new MqException("[Router] 交换机类型非法！");
        }
    }

    // 通过 DP 的方式重新实现这个方法. 实现思路参考 "动态规划精品课" 里的 "44.两个数组的 dp 问题_通配符匹配_Java"
    //todo 自己写一遍
    private boolean routeTopic(Binding binding, Message message) {
        // 按照 . 来切分 binding key 和 routing key

        // 无通配符
        String[] routingTokens = message.getRoutingKey().split("\\.");
        // 有通配符
        String[] bindingTokens = binding.getBindingKey().split("\\.");
        int m = routingTokens.length;
        int n = bindingTokens.length;

        // 1. 初始化 dp 表. 由于要考虑空串, dp 表的长和宽都要 + 1
        // dp[i][j] 表示的含义是 bindingTokens 中的 [0, j] 能否和 routingTokens 中的 [0, i] 匹配.
        boolean[][] dp = new boolean[m + 1][n + 1];
        // 空的 bindingKey 和 空的 routingKey 可以匹配
        dp[0][0] = true;
        // 如果 routingKey 为空, bindingKey 只有连续为 # 的时候, 才能匹配.
        for (int j = 1; j <= n; j++) {
            if (bindingTokens[j - 1].equals("#")) {
                dp[0][j] = true;
            } else {
                break;
            }
        }
        // 2. 遍历所有情况
        for (int i = 1; i <= m; i++) {
            for (int j = 1; j <= n; j++) {
                if (bindingTokens[j - 1].equals("#")) {
                    // 这块的状态转移方程推导过程很复杂. 参考算法视频讲解
                    dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
                } else if (bindingTokens[j - 1].equals("*")) {
                    // 如果 bindingTokens j 位置为 *, 那么 bindingTokens j - 1 位置和 routingKey i - 1 位置匹配即可.
                    dp[i][j] = dp[i - 1][j - 1];
                } else {
                    // 如果 bindingTokens j 位置为普通字符串, 那么要求 bindingTokens j - 1 位置 和 routingKey i - 1
                    // 位置匹配
                    // 并且 bindingTokens j 位置和 routingKey i 位置相同, 才认为是匹配
                    if (bindingTokens[j - 1].equals(routingTokens[i - 1])) {
                        dp[i][j] = dp[i - 1][j - 1];
                    } else {
                        dp[i][j] = false;
                    }
                }
            }
        }
        // 3. 处理返回值, 直接返回 dp 表的最后一个位置
        return dp[m][n];
    }

}
