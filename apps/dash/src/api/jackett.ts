import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { api } from "@/lib/api";

export type JackettAPIKey = {
  api_key: string;
};

export type UpdateJackettAPIKeyResult = {
  restarting: boolean;
  updated_indexers: number;
};

export function useJackettAPIKey() {
  return useQuery({
    queryFn: getJackettAPIKey,
    queryKey: ["/jackett/api-key"],
  });
}

export function useUpdateJackettAPIKeyMutation() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: updateJackettAPIKey,
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["/jackett/api-key"] });
    },
  });
}

async function getJackettAPIKey() {
  const { data } = await api<JackettAPIKey>("/jackett/api-key");
  return data;
}

async function updateJackettAPIKey(apiKey: string) {
  const { data } = await api<UpdateJackettAPIKeyResult>(
    "PUT /jackett/api-key",
    {
      body: { api_key: apiKey },
    },
  );
  return data;
}
