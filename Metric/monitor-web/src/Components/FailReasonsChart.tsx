import React from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import type { FailReasonChartData } from '../types';
import { formatNumber } from '../utils/format';

interface Props {
    chartData: FailReasonChartData[];
    keys: string[];
}

const COLORS = ['#EF4444', '#F59E0B', '#3B82F6', '#10B981', '#8B5CF6'];
const OTHER_COLOR = '#6B7280';
const TOP_N_LIMIT = 5;

const FailReasonsChart: React.FC<Props> = ({ chartData, keys }) => {
    if (!chartData || chartData.length === 0) return null;

    return (
        <div className="bg-gray-900 rounded-lg p-6 shadow-lg border border-gray-800">
        <div className="mb-4">
            <h3 className="text-white text-xl font-bold">Daily Failure Breakdown</h3>
            <p className="text-gray-400 text-sm mt-1">Showing top {Math.min(keys.length, TOP_N_LIMIT)} error types.</p>
        </div>
        <div className="h-[400px] w-full">
            <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData.slice(-8)}>
                <CartesianGrid strokeDasharray="3 3" stroke="#374151" vertical={false} />
                <XAxis dataKey="displayDate" stroke="#9CA3AF" tick={{ fill: '#9CA3AF' }} />
                <YAxis stroke="#9CA3AF" tickFormatter={formatNumber} tick={{ fill: '#9CA3AF' }} />
                <Tooltip
                cursor={{ fill: '#ffffff', opacity: 0.05 }}
                contentStyle={{ backgroundColor: '#1F2937', border: 'none', borderRadius: '8px', color: '#fff' }}
                formatter={(value: number) => value.toLocaleString()}
                />
                <Legend wrapperStyle={{ paddingTop: '10px' }} iconType="circle" />
                {keys.map((key, index) => (
                <Bar
                    key={key}
                    dataKey={key}
                    stackId="a"
                    fill={key === 'Others' ? OTHER_COLOR : COLORS[index % COLORS.length]}
                    name={key}
                />
                ))}
            </BarChart>
            </ResponsiveContainer>
        </div>
        </div>
    );
};

export default FailReasonsChart;