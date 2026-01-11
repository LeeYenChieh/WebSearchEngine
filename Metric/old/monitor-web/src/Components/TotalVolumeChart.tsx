import React from 'react';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Activity } from 'lucide-react';
import type { ServerDailyStat } from '../types';
import { formatNumber } from '../utils/format';

interface Props {
    data: ServerDailyStat[];
}

const TotalVolumeChart: React.FC<Props> = ({ data }) => {
    if (!data || data.length === 0) return null;

    return (
        <div className="bg-gray-900 rounded-lg p-6 shadow-lg border border-gray-800">
            <h3 className="text-white text-xl font-bold mb-4 flex items-center gap-2">
                <Activity className="w-5 h-5 text-blue-500" />
                Total Volume Overview
            </h3>

            <div className="h-[400px] w-full">
                <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={data.slice(-8)}>
                        <defs>
                            <linearGradient id="colorDiscovered" x1="0" y1="0" x2="0" y2="1">
                                <stop offset="5%" stopColor="#32b363ff" stopOpacity={0.3} />
                                <stop offset="95%" stopColor="#32b363ff" stopOpacity={0} />
                            </linearGradient>
                        </defs>

                        <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
                        <XAxis dataKey="displayDate" stroke="#9CA3AF" tick={{ fill: '#9CA3AF' }} />
                        <YAxis stroke="#9CA3AF" tickFormatter={formatNumber} tick={{ fill: '#9CA3AF' }} />

                        <Tooltip
                            content={({ active, payload, label }) => {
                                if (!active || !payload) return null;

                                const order = ["discovered", "crawled", "indexed"];
                                const nameMap: Record<string, string> = {
                                    discovered: "Discovered",
                                    crawled: "Crawled",
                                    indexed: "Indexed",
                                };
                                const colorMap: Record<string, string> = {
                                    discovered: "#32b363ff",
                                    crawled: "#f5760eff",
                                    indexed: "#8B5CF6",
                                };

                                // 🔥 依照自訂順序排序 tooltip 內容
                                const sorted = [...payload].sort(
                                    (a, b) => order.indexOf(a.dataKey as string) - order.indexOf(b.dataKey as string)
                                );

                                return (
                                    <div
                                        style={{
                                            backgroundColor: '#1F2937',
                                            borderRadius: '8px',
                                            padding: '10px',
                                            color: '#fff',
                                        }}
                                    >
                                        <div style={{ marginBottom: 6 }}>{label}</div>
                                        {sorted.map((entry) => (
                                            <div key={entry.dataKey} style={{ display: "flex", gap: 8, marginTop: 4 }}>
                                                <span
                                                    style={{
                                                        width: 10,
                                                        height: 10,
                                                        backgroundColor: colorMap[entry.dataKey],
                                                        borderRadius: 2,
                                                        display: "inline-block",
                                                    }}
                                                />
                                                <span>{nameMap[entry.dataKey]}</span>
                                                <span>{entry.value?.toLocaleString()}</span>
                                            </div>
                                        ))}
                                    </div>
                                );
                            }}
                        />

                        {/* 🔥 自訂 Legend 排序 — 不再依照英文字母排序 */}
                        <Legend
                            wrapperStyle={{ paddingTop: '10px' }}
                            content={() => {
                                const order = ["Discovered", "Crawled", "Indexed"];

                                const colorMap: Record<string, string> = {
                                    Discovered: "#32b363ff",
                                    Crawled: "#f5760eff",
                                    Indexed: "#8B5CF6",
                                };

                                return (
                                    <div style={{ display: "flex", gap: "20px", paddingTop: "10px" }}>
                                        {order.map((name) => (
                                            <span key={name} style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                                                <span
                                                    style={{
                                                        width: 12,
                                                        height: 12,
                                                        backgroundColor: colorMap[name],
                                                        borderRadius: 2,
                                                        display: "inline-block",
                                                    }}
                                                />
                                                <span style={{ color: "#fff" }}>{name}</span>
                                            </span>
                                        ))}
                                    </div>
                                );
                            }}
                        />

                        <Area type="monotone" dataKey="discovered" stroke="#32b363ff" fillOpacity={1} fill="url(#colorDiscovered)" strokeWidth={2} name="Discovered" dot={{ r: 3 }} />
                        <Area type="monotone" dataKey="crawled" stroke="#f5760eff" fill="none" strokeWidth={2} name="Crawled" dot={{ r: 3 }} />
                        <Area type="monotone" dataKey="indexed" stroke="#8B5CF6" fill="none" strokeWidth={2} name="Indexed" dot={{ r: 3 }} />
                    </AreaChart>
                </ResponsiveContainer>
            </div>
        </div>
    );
};

export default TotalVolumeChart;
