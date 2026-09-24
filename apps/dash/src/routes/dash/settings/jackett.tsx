import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { toast } from "sonner";

import { useJackettAPIKey, useUpdateJackettAPIKeyMutation } from "@/api/jackett";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { extractErrorMessages } from "@/lib/api";

export const Route = createFileRoute("/dash/settings/jackett")({
  component: RouteComponent,
  staticData: {
    crumb: "Jackett",
  },
});

function RouteComponent() {
  const { data, isLoading } = useJackettAPIKey();
  const update = useUpdateJackettAPIKeyMutation();
  const [apiKey, setApiKey] = useState("");

  useEffect(() => {
    if (data?.api_key) {
      setApiKey(data.api_key);
    }
  }, [data?.api_key]);

  const handleUpdate = () => {
    const trimmed = apiKey.trim();
    if (!trimmed) {
      toast.error("API Key cannot be empty");
      return;
    }
    toast.promise(update.mutateAsync(trimmed), {
      error(err: unknown) {
        return extractErrorMessages(err).join(", ") || "Failed to update";
      },
      loading: "Updating Jackett API Key...",
      success(result) {
        return `Updated ${result.updated_indexers} indexer(s) - restarting stremthru now`;
      },
    });
  };

  return (
    <div className="flex flex-col gap-6">
      <h2 className="text-lg font-semibold">Jackett</h2>

      <div className="flex flex-col gap-4">
        <div className="flex flex-col gap-2 max-w-md">
          <Label htmlFor="jackett-api-key">API Key</Label>
          <Input
            id="jackett-api-key"
            disabled={isLoading}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder="Jackett API Key"
            value={apiKey}
          />
        </div>
        <div>
          <Button disabled={update.isPending || isLoading} onClick={handleUpdate}>
            Update
          </Button>
        </div>
        <p className="text-muted-foreground text-sm max-w-md">
          Prefilled from Jackett's own config. If you rotate the key in
          Jackett's admin panel, paste the new value here and click Update -
          it pushes the key into every indexer stremthru uses for background
          caching, syncs the seedbox automation scripts, and restarts
          stremthru so the change takes effect immediately instead of
          waiting on the next scheduled health-check cycle.
        </p>
      </div>
    </div>
  );
}
