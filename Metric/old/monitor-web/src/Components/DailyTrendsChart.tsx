/* eslint-disable @typescript-eslint/no-explicit-any */
import React from 'react';
import { ComposedChart, Bar, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import type { ServerDailyStat } from '../types';
import { formatNumber, formatPercent } from '../utils/format';

interface Props {
    data: ServerDailyStat[];
}

const DailyTrendsChart: React.FC<Props> = ({ data }) => {
    if (!data || data.length === 0) return null;

    return (
        <div className="bg-gray-900 rounded-lg p-6 shadow-lg border border-gray-800">
        <h3 className="text-white text-xl font-bold mb-4">Crawl Throughput & Health</h3>
        <div className="h-[400px] w-full">
            <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={data.slice(-8)}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
                <XAxis dataKey="displayDate" stroke="#9CA3AF" tick={{ fill: '#9CA3AF' }} />
                <YAxis yAxisId="left" stroke="#9CA3AF" tickFormatter={formatNumber} tick={{ fill: '#9CA3AF' }} />
                <YAxis yAxisId="right" orientation="right" stroke="#F87171" tickFormatter={formatPercent} tick={{ fill: '#F87171' }} />
                <Tooltip
                contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px', color: '#fff' }}
                formatter={(value: any, name: any) => {
                    if (name === 'Failed Rate') return formatPercent(Number(value));
                    return Number(value).toLocaleString();
                }}
                />
                <Legend wrapperStyle={{ paddingTop: '10px' }} />
                <Bar yAxisId="left" dataKey="fetch_ok" name="Fetch OK" stackId="a" fill="#10B981" radius={[0, 0, 4, 4]} />
                <Bar yAxisId="left" dataKey="fetch_fail" name="Fetch Fail" stackId="a" fill="#EF4444" radius={[4, 4, 0, 0]} />
                <Line yAxisId="left" type="monotone" dataKey="new_links" name="New Links" stroke="#3B82F6" strokeWidth={2} dot={{ r: 3 }} />
                <Line yAxisId="right" type="monotone" dataKey="failed_rate" name="Failed Rate" stroke="#F59E0B" strokeWidth={2} strokeDasharray="5 5" dot={{ r: 3 }} />
            </ComposedChart>
            </ResponsiveContainer>
        </div>
        </div>
    );
};

export default DailyTrendsChart;