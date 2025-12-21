/* eslint-disable react-hooks/static-components */
/* eslint-disable @typescript-eslint/no-explicit-any */
import React from 'react';
import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from 'recharts';
import { format, parseISO } from 'date-fns';

// 定義傳入的資料型別
interface MetricData {
    date: string;
    discover_find: number;
    fetch_find: number;
    upload_find: number;
    rank_find: number;
    total: number;
}

interface MetricTrendChartProps {
    title: string;
    data: MetricData[];
}

const MetricTrendChart: React.FC<MetricTrendChartProps> = ({ title, data }) => {
    // 客製化 Tooltip 來同時顯示 "188 / 797" 和 "23.5%"
    const CustomTooltip = ({ active, payload, label }: any) => {
        if (active && payload && payload.length) {
        // 從 payload 中取得當天的 total (因為 total 在每一行數據裡都是一樣的)
        const currentData = payload[0].payload; 
        const total = currentData.total;

        return (
            <div className="bg-gray-900 border border-gray-700 p-4 rounded shadow-xl">
            <p className="text-gray-300 font-bold mb-2">{label} (Total: {total})</p>
            {payload.map((entry: any) => {
                const value = entry.value; // 百分比
                const rawValue = currentData[`${entry.dataKey.split('_')[0]}_find`]; // 原始數值
                
                return (
                <div key={entry.name} className="flex items-center gap-2 mb-1" style={{ color: entry.color }}>
                    <span className="w-2 h-2 rounded-full" style={{ backgroundColor: entry.color }}></span>
                    <span className="font-medium capitalize">{entry.name}: </span>
                    <span>
                    {/* 格式要求: f'{find} / {total}' 和 f'{percentage}%' */}
                    {rawValue} / {total} ({value.toFixed(2)}%)
                    </span>
                </div>
                );
            })}
            </div>
        );
        }
        return null;
    };

    return (
        <div className="bg-gray-900 p-6 rounded-xl border border-gray-800 shadow-sm">
        <h3 className="text-lg font-semibold text-gray-200 mb-4">{title}</h3>
        <div className="h-[300px] w-full">
            <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data.slice(-8)} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
                <XAxis
                    dataKey="date"
                    stroke="#9CA3AF"
                    tickFormatter={(str) => format(parseISO(str), 'MM-dd')}
                    tick={{ fontSize: 12 }}
                />
                <YAxis
                    stroke="#9CA3AF"
                    unit="%"
                    tick={{ fontSize: 12 }}
                />
                <Tooltip content={<CustomTooltip />} />
                
                <Legend
                    wrapperStyle={{ paddingTop: '10px' }}
                    content={() => {
                        // 定義顯示順序
                        const order = ["Discovered", "Crawled", "Indexed", "Ranked"];

                        // 定義顏色映射 (包含原本的 Ranked 黃色)
                        const colorMap: Record<string, string> = {
                            Discovered: "#32b363ff", // 新顏色
                            Crawled: "#f5760eff",    // 新顏色
                            Indexed: "#8B5CF6",      // 紫色
                            Ranked: "#F59E0B",       // 原本的黃色
                        };

                        return (
                            <div style={{ display: "flex", gap: "20px", paddingTop: "10px", justifyContent: "center" }}>
                                {order.map((name) => (
                                    <span key={name} style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                                        <span
                                            style={{
                                                width: 12,
                                                height: 12,
                                                backgroundColor: colorMap[name],
                                                borderRadius: 2, // 如果想要圓形改成 '50%'
                                                display: "inline-block",
                                            }}
                                        />
                                        <span style={{ color: "#d1d5db", fontSize: "14px", fontWeight: 500 }}>{name}</span>
                                    </span>
                                ))}
                            </div>
                        );
                    }}
                />

                {/* 注意：這裡的 stroke 顏色必須手動更新，以符合上面 Legend 定義的顏色 
                */}
                <Line 
                    type="monotone" 
                    dataKey="discover_pct" 
                    name="Discovered" 
                    stroke="#32b363ff" 
                    strokeWidth={2} 
                    dot={{ r: 3 }} 
                    activeDot={{ r: 6 }} 
                />
                <Line 
                    type="monotone" 
                    dataKey="fetch_pct" 
                    name="Crawled" 
                    stroke="#f5760eff" 
                    strokeWidth={2} 
                    dot={{ r: 3 }} 
                />
                <Line 
                    type="monotone" 
                    dataKey="upload_pct" 
                    name="Indexed" 
                    stroke="#8B5CF6" 
                    strokeWidth={2} 
                    dot={{ r: 3 }} 
                />
                <Line 
                    type="monotone" 
                    dataKey="rank_pct" 
                    name="Ranked" 
                    stroke="#F59E0B" 
                    strokeWidth={2} 
                    dot={{ r: 3 }} 
                />
            </LineChart>
            </ResponsiveContainer>
        </div>
        </div>
    );
};

export default MetricTrendChart;